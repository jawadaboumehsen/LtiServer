package io.ltirom.server.engine

import io.ltirom.server.domain.ports.ProcessCancellationPort
import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.domain.ports.ProcessOutputChunk
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.model.RunStartOutcome
import io.ltirom.server.model.StartRunRequest
import io.ltirom.server.model.ToolExecutionRequest
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class ForcedShutdownTest {
    private class Resolver : ToolResolverPort {
        override fun resolve(toolName: String): File? = File("/fake/$toolName")

        override fun listAvailableTools(): Map<String, Boolean> = mapOf("sleep" to true)
    }

    private class Endless : ProcessExecutionPort {
        override suspend fun runProcess(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?,
            executionId: String?,
        ): ProcessExecutionResult = awaitCancellation()

        override fun runProcessFlow(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            executionId: String?,
        ): Flow<ProcessOutputChunk> = flow { awaitCancellation() }
    }

    /** A runner whose cancellation request takes seconds to return, as one waiting on a stuck process does. */
    private class SlowToCancel : ProcessExecutionPort by Endless(), ProcessCancellationPort {
        override fun cancelProcess(executionId: String, signal: String): Boolean {
            Thread.sleep(3_000)
            return true
        }
    }

    private fun request(key: String) = StartRunRequest(
        ToolExecutionRequest("sleep", emptyList(), workingDirectory = File(".").absolutePath),
        key,
    )

    @Test
    fun `stopping all work cancels active runs and waits until nothing is tracked`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(Resolver(), Endless(), "Ubuntu-Test")
        val first = assertIs<RunStartOutcome.Created>(engine.startRun(request("a"))).handle.runId
        val second = assertIs<RunStartOutcome.Created>(engine.startRun(request("b"))).handle.runId
        withTimeout(5_000) { while (engine.activeWork().size != 2) delay(10) }

        val active = engine.closeAdmission(force = true)
        assertEquals(setOf(first, second), active.toSet())

        val remaining = engine.cancelWorkAndWait(active, waitMs = 5_000)

        assertTrue(remaining.isEmpty(), "still active after the forced stop: $remaining")
    }

    @Test
    fun `work that does not stop is reported after the wait instead of being waited for forever`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(Resolver(), Endless(), "Ubuntu-Test")
        // A legacy execution the runner cannot cancel (no cancellation port): it stays tracked.
        val scope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.Dispatchers.Default)
        val stuck = scope.launch {
            engine.execute(
                ToolExecutionRequest("sleep", emptyList(), workingDirectory = File(".").absolutePath),
                "stuck-1",
            )
        }
        withTimeout(5_000) { while (engine.activeWork().isEmpty()) delay(10) }

        val started = System.nanoTime()
        val remaining = engine.cancelWorkAndWait(engine.activeWork(), waitMs = 400)
        val tookMs = (System.nanoTime() - started) / 1_000_000

        assertTrue(remaining.isNotEmpty())
        assertTrue(tookMs < 2_000, "gave up after the limit, took ${tookMs}ms")
        stuck.cancel()
    }

    @Test
    fun `slow cancellations of several runs do not stretch the stop beyond its budget`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(Resolver(), SlowToCancel(), "Ubuntu-Test")
        repeat(3) { assertIs<RunStartOutcome.Created>(engine.startRun(request("slow-$it"))) }
        withTimeout(5_000) { while (engine.activeWork().size != 3) delay(10) }

        val started = System.nanoTime()
        engine.cancelWorkWithin(engine.activeWork(), budgetMs = 500)
        val boundedMs = (System.nanoTime() - started) / 1_000_000
        assertTrue(boundedMs < 1_500, "three 3 s cancellations, one 500 ms budget: took ${boundedMs}ms")

        val again = System.nanoTime()
        engine.cancelWorkAndWait(engine.activeWork(), waitMs = 800)
        val totalMs = (System.nanoTime() - again) / 1_000_000
        assertTrue(totalMs < 2_000, "cancel and wait share one budget, took ${totalMs}ms")
    }
}
