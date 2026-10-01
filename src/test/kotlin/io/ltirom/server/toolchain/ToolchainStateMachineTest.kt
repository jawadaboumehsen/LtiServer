package io.ltirom.server.toolchain

import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.engine.DefaultServerEngine
import io.ltirom.server.model.RunPurpose
import io.ltirom.server.model.RunStartOutcome
import io.ltirom.server.model.StartRunRequest
import io.ltirom.server.model.ToolExecutionRequest
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class ToolchainStateMachineTest {

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

    private fun request(key: String, purpose: RunPurpose? = null): StartRunRequest {
        val execReq = if (purpose != null) {
            ToolExecutionRequest(
                toolId = "sleep",
                arguments = emptyList(),
                workingDirectory = File(".").absolutePath,
                purpose = purpose
            )
        } else {
            ToolExecutionRequest(
                toolId = "sleep",
                arguments = emptyList(),
                workingDirectory = File(".").absolutePath
            )
        }
        return if (purpose != null) {
            StartRunRequest(request = execReq, idempotencyKey = key, purpose = purpose)
        } else {
            StartRunRequest(request = execReq, idempotencyKey = key)
        }
    }

    @Test
    fun `default purpose is WORKSPACE`() {
        val req = StartRunRequest(
            request = ToolExecutionRequest(toolId = "sleep", arguments = emptyList()),
            idempotencyKey = "k1"
        )
        assertEquals(RunPurpose.WORKSPACE, req.purpose)
        assertEquals(RunPurpose.WORKSPACE, req.request.purpose)
    }

    @Test
    fun `UNINITIALIZED state admits SETUP and refuses WORKSPACE with TOOLCHAIN_UNINITIALIZED`() = runBlocking<Unit> {
        val stateMachine = MutableToolchainStateMachine(ToolchainState.UNINITIALIZED)
        assertTrue(stateMachine.canAdmit(RunPurpose.SETUP))
        assertFalse(stateMachine.canAdmit(RunPurpose.WORKSPACE))

        val engine = DefaultServerEngine(
            toolResolver = FakeResolver(),
            processRunner = BlockingRunner(),
            distroName = "Ubuntu-Test",
            toolchainStateMachine = stateMachine
        )

        // SETUP admitted
        val setupOutcome = engine.startRun(request("setup-1", RunPurpose.SETUP))
        assertIs<RunStartOutcome.Created>(setupOutcome)

        // Default purpose (WORKSPACE) refused with ToolchainNotReady
        val workspaceDefaultOutcome = engine.startRun(request("ws-def"))
        val notReadyDef = assertIs<RunStartOutcome.ToolchainNotReady>(workspaceDefaultOutcome)
        assertEquals(ToolchainState.UNINITIALIZED, notReadyDef.state)
        assertEquals("TOOLCHAIN_UNINITIALIZED", notReadyDef.errorCode)

        // Explicit WORKSPACE refused
        val workspaceExplicitOutcome = engine.startRun(request("ws-exp", RunPurpose.WORKSPACE))
        val notReadyExp = assertIs<RunStartOutcome.ToolchainNotReady>(workspaceExplicitOutcome)
        assertEquals(ToolchainState.UNINITIALIZED, notReadyExp.state)
        assertEquals("TOOLCHAIN_UNINITIALIZED", notReadyExp.errorCode)
    }

    @Test
    fun `RECOVERY_REQUIRED state admits SETUP and refuses WORKSPACE with TOOLCHAIN_RECOVERY`() = runBlocking<Unit> {
        val stateMachine = MutableToolchainStateMachine(ToolchainState.RECOVERY_REQUIRED)
        assertTrue(stateMachine.canAdmit(RunPurpose.SETUP))
        assertFalse(stateMachine.canAdmit(RunPurpose.WORKSPACE))

        val engine = DefaultServerEngine(
            toolResolver = FakeResolver(),
            processRunner = BlockingRunner(),
            distroName = "Ubuntu-Test",
            toolchainStateMachine = stateMachine
        )

        // SETUP admitted
        val setupOutcome = engine.startRun(request("setup-rec", RunPurpose.SETUP))
        assertIs<RunStartOutcome.Created>(setupOutcome)

        // WORKSPACE refused
        val workspaceOutcome = engine.startRun(request("ws-rec", RunPurpose.WORKSPACE))
        val notReady = assertIs<RunStartOutcome.ToolchainNotReady>(workspaceOutcome)
        assertEquals(ToolchainState.RECOVERY_REQUIRED, notReady.state)
        assertEquals("TOOLCHAIN_RECOVERY", notReady.errorCode)
    }

    @Test
    fun `READY state admits both SETUP and WORKSPACE`() = runBlocking<Unit> {
        val stateMachine = MutableToolchainStateMachine(ToolchainState.READY)
        assertTrue(stateMachine.canAdmit(RunPurpose.SETUP))
        assertTrue(stateMachine.canAdmit(RunPurpose.WORKSPACE))

        val engine = DefaultServerEngine(
            toolResolver = FakeResolver(),
            processRunner = BlockingRunner(),
            distroName = "Ubuntu-Test",
            toolchainStateMachine = stateMachine
        )

        val setupOutcome = engine.startRun(request("setup-ready", RunPurpose.SETUP))
        assertIs<RunStartOutcome.Created>(setupOutcome)

        val workspaceOutcome = engine.startRun(request("ws-ready", RunPurpose.WORKSPACE))
        assertIs<RunStartOutcome.Created>(workspaceOutcome)
    }
}
