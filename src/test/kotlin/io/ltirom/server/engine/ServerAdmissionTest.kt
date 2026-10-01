package io.ltirom.server.engine

import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.model.RunStartOutcome
import io.ltirom.server.model.StartRunRequest
import io.ltirom.server.model.ToolExecutionRequest
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** A shutdown decision and run admission are one atomic step, so an accepted shutdown never kills new work. */
class ServerAdmissionTest {

    private class FakeResolver : ToolResolverPort {
        override fun resolve(toolName: String): File? = File("/fake/$toolName")
        override fun listAvailableTools(): Map<String, Boolean> = mapOf("sleep" to true)
    }

    /** A tool that runs until cancelled, so its run stays active. */
    private class BlockingRunner : ProcessExecutionPort {
        override suspend fun runProcess(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?,
        ): ProcessExecutionResult = awaitCancellation()
    }

    private fun request(key: String) = StartRunRequest(
        request = ToolExecutionRequest(toolId = "sleep", arguments = emptyList(), workingDirectory = File(".").absolutePath),
        idempotencyKey = key,
    )

    @Test
    fun `an idle service closes admission and refuses runs that arrive after the shutdown decision`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(FakeResolver(), BlockingRunner(), "Ubuntu-Test")

        assertTrue(engine.closeAdmission().isEmpty(), "No active runs: admission closes")
        assertEquals(RunStartOutcome.ShuttingDown, engine.startRun(request("late")))
    }

    @Test
    fun `an active run blocks the shutdown and admission stays open`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(FakeResolver(), BlockingRunner(), "Ubuntu-Test")
        assertIs<RunStartOutcome.Created>(engine.startRun(request("busy")))

        val active = engine.closeAdmission()

        assertEquals(1, active.size, "The running job must block the shutdown")
        assertIs<RunStartOutcome.Created>(engine.startRun(request("next")), "Admission must stay open")
    }

    @Test
    fun `a forced shutdown closes admission even with active runs`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(FakeResolver(), BlockingRunner(), "Ubuntu-Test")
        engine.startRun(request("busy"))

        engine.closeAdmission(force = true)

        assertEquals(RunStartOutcome.ShuttingDown, engine.startRun(request("late")))
    }

    private suspend fun awaitActive(engine: DefaultServerEngine, count: Int) = withTimeout(5_000) {
        while (engine.getHealth().activeRuns != count) delay(10)
    }

    @Test
    fun `a legacy execute in flight blocks the shutdown and later calls are refused`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(FakeResolver(), BlockingRunner(), "Ubuntu-Test")
        val job = launch(Dispatchers.Default) { engine.execute(request("x").request, "trace-1") }
        awaitActive(engine, 1)

        assertEquals(listOf("execute:trace-1"), engine.closeAdmission(), "Legacy work counts as active")

        job.cancel()
        job.join()
        assertTrue(engine.closeAdmission().isEmpty(), "Idle again: admission closes")
        assertFailsWith<ServiceShuttingDownException> { engine.execute(request("y").request) }
    }

    @Test
    fun `a legacy stream in flight blocks the shutdown and later streams are refused`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(FakeResolver(), BlockingRunner(), "Ubuntu-Test")
        val job = launch(Dispatchers.Default) { engine.stream(request("x").request, sessionId = "s1").collect() }
        awaitActive(engine, 1)

        assertEquals(listOf("stream:s1"), engine.closeAdmission())

        job.cancel()
        job.join()
        assertTrue(engine.closeAdmission().isEmpty())
        assertFailsWith<ServiceShuttingDownException> { engine.stream(request("y").request).collect() }
    }

    @Test
    fun `concurrent requests with one idempotency key create exactly one run`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(FakeResolver(), BlockingRunner(), "Ubuntu-Test")

        val outcomes = (1..32).map { async(Dispatchers.Default) { engine.startRun(request("same-key")) } }.awaitAll()

        assertEquals(1, outcomes.count { it is RunStartOutcome.Created }, "$outcomes")
        assertEquals(31, outcomes.count { it is RunStartOutcome.Existing })
    }

    @Test
    fun `concurrent requests for one workspace admit exactly one run`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(FakeResolver(), BlockingRunner(), "Ubuntu-Test")

        val outcomes = (1..32).map { i ->
            async(Dispatchers.Default) { engine.startRun(request("key-$i").copy(workspaceLock = "/ws")) }
        }.awaitAll()

        assertEquals(1, outcomes.count { it is RunStartOutcome.Created }, "$outcomes")
        assertEquals(31, outcomes.count { it is RunStartOutcome.Conflict })
    }
}
