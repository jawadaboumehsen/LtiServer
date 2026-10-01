package io.ltirom.server.application

import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.model.StreamEvent
import io.ltirom.server.model.ToolExecutionRequest
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ServerUseCasesTest {

    private class FakeToolResolver(private val available: Set<String>) : ToolResolverPort {
        override fun resolve(toolName: String): File? =
            if (toolName in available) File("/fake/bin/$toolName") else null

        override fun listAvailableTools(): Map<String, Boolean> =
            available.associateWith { true }
    }

    private class FakeProcessRunner(
        private val exitCode: Int = 0,
        private val stdout: String = "simulated stdout",
        private val stderr: String = "",
        private val durationMs: Long = 42L
    ) : ProcessExecutionPort {
        override suspend fun runProcess(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?
        ): ProcessExecutionResult {
            onOutput?.invoke(stdout, stderr)
            return ProcessExecutionResult(exitCode, stdout, stderr, durationMs)
        }
    }

    @Test
    fun `ExecuteToolUseCase executes tool when resolved`() = runBlocking {
        val resolver = FakeToolResolver(setOf("mke2fs"))
        val runner = FakeProcessRunner(exitCode = 0, stdout = "mke2fs 1.47.0")
        val useCase = ExecuteToolUseCase(resolver, runner)

        val response = useCase.execute(
            ToolExecutionRequest(
                toolId = "mke2fs",
                arguments = listOf("-V"),
                workingDirectory = File(".").absolutePath
            )
        )
        assertEquals(0, response.exitCode)
        assertEquals("mke2fs 1.47.0", response.stdout)
        assertEquals(42L, response.durationMs)
    }

    @Test
    fun `ExecuteToolUseCase returns error when tool is not found`() = runBlocking {
        val resolver = FakeToolResolver(emptySet())
        val runner = FakeProcessRunner()
        val useCase = ExecuteToolUseCase(resolver, runner)

        val response = useCase.execute(
            ToolExecutionRequest(
                toolId = "nonexistent_tool",
                arguments = emptyList(),
                workingDirectory = File(".").absolutePath
            )
        )
        assertEquals(-1, response.exitCode)
        assertTrue(response.stderr.contains("not found"))
    }

    @Test
    fun `StreamToolUseCase streams events to callback`() = runBlocking {
        val resolver = FakeToolResolver(setOf("simg2img"))
        val runner = FakeProcessRunner(exitCode = 0, stdout = "converting block 100/100")
        val useCase = StreamToolUseCase(resolver, runner)

        val events = mutableListOf<StreamEvent>()
        useCase.stream(
            ToolExecutionRequest(
                toolId = "simg2img",
                arguments = listOf("system.img", "system.raw"),
                workingDirectory = File(".").absolutePath
            )
        ) {
            events.add(it)
        }

        assertTrue(events.isNotEmpty())
        val last = events.last()
        assertTrue(last is StreamEvent.ExecutionFinished)
        assertEquals(0, (last as StreamEvent.ExecutionFinished).exitCode)
    }

    @Test
    fun `GetServerHealthUseCase gathers system and tool metrics`() {
        val resolver = FakeToolResolver(setOf("aapt2", "adb"))
        val useCase = GetServerHealthUseCase(resolver, distroName = "Ubuntu-Fake")

        val health = useCase.getHealth()
        assertEquals("UP", health.status)
        assertEquals("Ubuntu-Fake", health.distro)
        assertEquals(listOf("aapt2", "adb"), health.availableTools)
    }

    @Test
    fun `CancelProcessUseCase dispatches cancellation signal`() {
        var cancelledId: String? = null
        val cancellationPort = object : io.ltirom.server.domain.ports.ProcessCancellationPort {
            override fun cancelProcess(executionId: String, signal: String): Boolean {
                cancelledId = executionId
                return true
            }
        }
        val useCase = CancelProcessUseCase(cancellationPort)
        val result = useCase.cancel("exec-12345", "SIGTERM")

        assertTrue(result)
        assertEquals("exec-12345", cancelledId)
    }
}

