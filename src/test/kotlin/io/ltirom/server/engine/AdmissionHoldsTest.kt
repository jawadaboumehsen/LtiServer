package io.ltirom.server.engine

import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.model.RunPurpose
import io.ltirom.server.model.RunStartOutcome
import io.ltirom.server.model.StartRunRequest
import io.ltirom.server.model.ToolExecutionRequest
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

class AdmissionHoldsTest {

    private class FakeResolver : ToolResolverPort {
        override fun resolve(toolName: String): File? = File("/fake/$toolName")
        override fun listAvailableTools(): Map<String, Boolean> = mapOf("sleep" to true)
    }

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

    private fun request(key: String, purpose: RunPurpose = RunPurpose.WORKSPACE) = StartRunRequest(
        request = ToolExecutionRequest(
            toolId = "sleep",
            arguments = emptyList(),
            workingDirectory = File(".").absolutePath,
            purpose = purpose
        ),
        idempotencyKey = key,
        purpose = purpose
    )

    @Test
    fun `adding a hold blocks new runs and legacy work until removed`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(FakeResolver(), BlockingRunner(), "Ubuntu-Test")

        assertTrue(engine.admissionHolds.isEmpty(), "Initially no holds")

        engine.addHold(Hold.Shutdown)
        assertEquals(setOf(Hold.Shutdown), engine.admissionHolds)

        assertEquals(RunStartOutcome.ShuttingDown, engine.startRun(request("r1")))
        assertFailsWith<ServiceShuttingDownException> { engine.execute(request("e1").request) }

        engine.removeHold(Hold.Shutdown)
        assertTrue(engine.admissionHolds.isEmpty(), "Holds empty after removal")

        assertIs<RunStartOutcome.Created>(engine.startRun(request("r2")))
    }

    @Test
    fun `activation early exit never removes SHUTDOWN hold`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(FakeResolver(), BlockingRunner(), "Ubuntu-Test")

        engine.addHold(Hold.Shutdown)

        val activationHold = Hold.Activation("act-123")
        engine.addHold(activationHold)
        assertEquals(setOf(Hold.Shutdown, activationHold), engine.admissionHolds)

        // Simulating early exit / failure in activation transaction: removes only its own hold
        engine.removeHold(activationHold)

        assertEquals(setOf(Hold.Shutdown), engine.admissionHolds)
        assertEquals(RunStartOutcome.ShuttingDown, engine.startRun(request("r1")))
        assertFailsWith<ServiceShuttingDownException> { engine.execute(request("e1").request) }

        // Finally remove shutdown
        engine.removeHold(Hold.Shutdown)
        assertTrue(engine.admissionHolds.isEmpty())
        assertIs<RunStartOutcome.Created>(engine.startRun(request("r3")))
    }

    private suspend fun awaitActive(engine: DefaultServerEngine, count: Int) = withTimeout(5_000) {
        while (engine.getHealth().activeRuns != count) delay(10)
    }

    @Test
    fun `atomic check no work and add hold rejects when active work exists and does not add hold`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(FakeResolver(), BlockingRunner(), "Ubuntu-Test")

        assertIs<RunStartOutcome.Created>(engine.startRun(request("running-job")))

        val activeWork = engine.tryAddHold(Hold.Activation("act-test"))
        assertEquals(1, activeWork.size, "Active run should block the hold")
        assertTrue(engine.admissionHolds.isEmpty(), "Hold must NOT be added when active work exists")

        // New runs are still admitted because hold was not taken
        assertIs<RunStartOutcome.Created>(engine.startRun(request("another-job")))
    }

    @Test
    fun `atomic check no work and add hold succeeds when idle`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(FakeResolver(), BlockingRunner(), "Ubuntu-Test")

        val activeWork = engine.tryAddHold(Hold.Activation("act-test"))
        assertTrue(activeWork.isEmpty(), "No active work: should return empty list")
        assertEquals(setOf(Hold.Activation("act-test")), engine.admissionHolds)

        // Now new runs are blocked
        assertEquals(RunStartOutcome.ShuttingDown, engine.startRun(request("blocked-job")))
    }

    @Test
    fun `atomic check no work detects in flight legacy execute`() = runBlocking<Unit> {
        val engine = DefaultServerEngine(FakeResolver(), BlockingRunner(), "Ubuntu-Test")

        val job = launch(Dispatchers.Default) { engine.execute(request("x").request, "trace-leg") }
        awaitActive(engine, 1)

        val activeWork = engine.tryAddHold(Hold.Activation("act-leg"))
        assertEquals(listOf("execute:trace-leg"), activeWork)
        assertTrue(engine.admissionHolds.isEmpty(), "Hold must not be added while legacy work is running")

        job.cancel()
        job.join()

        // Once idle, hold addition succeeds
        val idleWork = engine.tryAddHold(Hold.Activation("act-leg"))
        assertTrue(idleWork.isEmpty())
        assertEquals(setOf(Hold.Activation("act-leg")), engine.admissionHolds)
    }
}
