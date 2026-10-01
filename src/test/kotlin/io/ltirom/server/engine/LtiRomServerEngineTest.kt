package io.ltirom.server.engine

import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.model.ToolExecutionRequest
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
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
            onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?
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
}
