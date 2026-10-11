package io.ltirom.server.engine

import io.ltirom.server.domain.ports.ProcessCancellationPort
import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.model.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class LtiRomServerEngineTest {

    private class FakeResolver : ToolResolverPort {
        override fun resolve(toolName: String): File? = File("/fake/$toolName")
        override fun listAvailableTools(): Map<String, Boolean> = mapOf("fastboot" to true)
    }

    private class FakeRunner : ProcessExecutionPort {
        override suspend fun runProcess(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?,
            executionId: String?
        ): ProcessExecutionResult {
            return ProcessExecutionResult(0, "fastboot version 34.0.5", "", 15L)
        }
    }

    @Test
    fun `engine executes tool and returns structured response`() = runBlocking {
        val engine = DefaultServerEngine(FakeResolver(), FakeRunner(), "Ubuntu-Test")

        val res = engine.execute(
            ToolExecutionRequest(
                toolId = "fastboot",
                arguments = listOf("--version"),
                workingDirectory = File(".").absolutePath
            )
        )
        assertEquals(0, res.exitCode)
        assertTrue(res.stdout.contains("34.0.5"))

        val health = engine.getHealth()
        assertEquals("UP", health.status)
        assertEquals("Ubuntu-Test", health.distro)
    }

    @Test
    fun `engine streams sequenced events and creates session`() = runBlocking {
        val engine = DefaultServerEngine(FakeResolver(), FakeRunner(), "Ubuntu-Test")

        val events = engine.stream(
            ToolExecutionRequest(
                toolId = "fastboot",
                arguments = listOf("devices"),
                workingDirectory = File(".").absolutePath
            )
        ).toList()
        assertTrue(events.isNotEmpty())

        val sessions = engine.listSessions()
        assertEquals(1, sessions.size)
        assertEquals("fastboot", sessions.first().toolId)
    }

    @Test
    fun `cancel on running run during spawn terminates newly created process and settles as CANCELLED`() = runBlocking {
        val spawnGate = kotlinx.coroutines.CompletableDeferred<Unit>()
        val processStarted = kotlinx.coroutines.CompletableDeferred<Unit>()
        var processTerminated = false

        val runner = object : ProcessExecutionPort, ProcessCancellationPort {
            override suspend fun runProcess(
                toolName: String,
                arguments: List<String>,
                workingDir: File,
                environment: Map<String, String>,
                stdinText: String?,
                timeoutMs: Long,
                onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?,
                executionId: String?
            ): ProcessExecutionResult {
                processStarted.complete(Unit)
                spawnGate.await()
                return ProcessExecutionResult(0, "done", "", 10L)
            }

            override fun cancelProcess(executionId: String, signal: String): Boolean {
                processTerminated = true
                spawnGate.complete(Unit)
                return true
            }
        }

        val engine = DefaultServerEngine(FakeResolver(), runner, "Ubuntu-Test")

        val req = StartRunRequest(
            request = ToolExecutionRequest(
                toolId = "fastboot",
                arguments = listOf("devices"),
                workingDirectory = File(".").absolutePath
            ),
            idempotencyKey = "cancel-spawn-1"
        )
        val outcome = engine.startRun(req) as RunStartOutcome.Created
        val runId = outcome.handle.runId

        processStarted.await()

        val cancelResp = engine.cancelRun(runId, "SIGTERM")
        assertTrue(cancelResp.accepted)
        assertEquals(RunStatusValue.CANCELLING, cancelResp.status)

        delay(100)

        assertTrue(processTerminated)
        val run = engine.getRun(runId)
        assertEquals(RunStatusValue.CANCELLED, run?.status)
    }

    @Test
    fun `cancel on run with surviving processes settles as CANCELLED and reported in activeWork until survivors die`() = runBlocking {
        val processStarted = kotlinx.coroutines.CompletableDeferred<Unit>()
        val cancelReceived = kotlinx.coroutines.CompletableDeferred<Unit>()

        val runner = object : ProcessExecutionPort, ProcessCancellationPort {
            val survivingPids = mutableMapOf<String, MutableList<Long>>()

            override suspend fun runProcess(
                toolName: String,
                arguments: List<String>,
                workingDir: File,
                environment: Map<String, String>,
                stdinText: String?,
                timeoutMs: Long,
                onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?,
                executionId: String?
            ): ProcessExecutionResult {
                processStarted.complete(Unit)
                cancelReceived.await()
                throw kotlinx.coroutines.CancellationException("Cancelled with survivors")
            }

            override fun cancelProcess(executionId: String, signal: String): Boolean {
                survivingPids[executionId] = mutableListOf(9999L)
                cancelReceived.complete(Unit)
                return true
            }

            override fun getSurvivors(executionId: String): List<Long> {
                return survivingPids[executionId] ?: emptyList()
            }
        }

        val engine = DefaultServerEngine(FakeResolver(), runner, "Ubuntu-Test")

        val req = StartRunRequest(
            request = ToolExecutionRequest(
                toolId = "fastboot",
                arguments = listOf("devices"),
                workingDirectory = File(".").absolutePath
            ),
            idempotencyKey = "cancel-survivor-1"
        )
        val outcome = engine.startRun(req) as RunStartOutcome.Created
        val runId = outcome.handle.runId

        processStarted.await()

        val cancelResp = engine.cancelRun(runId, "SIGTERM")
        assertTrue(cancelResp.accepted)

        delay(100)

        val run = engine.getRun(runId)
        assertEquals(RunStatusValue.CANCELLED, run?.status)
        assertEquals(listOf(9999L), run?.unconfirmedSurvivors)
        assertTrue(engine.activeWork().contains(runId), "Run with survivors must remain in activeWork")

        // When survivor processes finally terminate:
        runner.survivingPids[runId]?.clear()
        assertFalse(engine.activeWork().contains(runId), "Active work must be clear once survivors terminate")
    }
}
