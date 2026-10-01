package io.ltirom.server.core

import io.ltirom.server.application.ExecuteToolUseCase
import io.ltirom.server.application.StreamToolUseCase
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.model.StreamEvent
import io.ltirom.server.model.ToolExecutionRequest
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ProcessRunnerWorkingDirTest {

    private class EchoToolResolver : ToolResolverPort {
        override fun resolve(toolName: String): File? {
            return if (toolName == "echo") {
                val path = if (System.getProperty("os.name").lowercase().contains("win")) {
                    File("C:\\Windows\\System32\\cmd.exe")
                } else {
                    File("/bin/echo")
                }
                if (path.exists()) path else File(".")
            } else null
        }

        override fun listAvailableTools(): Map<String, Boolean> = mapOf("echo" to true)
    }

    @Test
    fun `stream rejects null working directory without spawn`() = runBlocking {
        val resolver = EchoToolResolver()
        val runner = LinuxProcessRunner(resolver)
        val useCase = StreamToolUseCase(resolver, runner)

        val events = mutableListOf<StreamEvent>()
        val request = ToolExecutionRequest(
            toolId = "echo",
            arguments = listOf("hello"),
            workingDirectory = null
        )

        useCase.stream(request) { events.add(it) }

        val finished = events.filterIsInstance<StreamEvent.ExecutionFinished>().firstOrNull()
        assertTrue(finished != null, "Must emit ExecutionFinished")
        assertEquals(-1, finished.exitCode)
        assertEquals("working directory missing", finished.summary)
    }

    @Test
    fun `stream rejects relative working directory without spawn`() = runBlocking {
        val resolver = EchoToolResolver()
        val runner = LinuxProcessRunner(resolver)
        val useCase = StreamToolUseCase(resolver, runner)

        val events = mutableListOf<StreamEvent>()
        val request = ToolExecutionRequest(
            toolId = "echo",
            arguments = listOf("hello"),
            workingDirectory = "relative/sub/dir"
        )

        useCase.stream(request) { events.add(it) }

        val finished = events.filterIsInstance<StreamEvent.ExecutionFinished>().firstOrNull()
        assertTrue(finished != null, "Must emit ExecutionFinished")
        assertEquals(-1, finished.exitCode)
        assertEquals("working directory missing", finished.summary)
    }

    @Test
    fun `stream rejects non-existent working directory without spawn`() = runBlocking {
        val resolver = EchoToolResolver()
        val runner = LinuxProcessRunner(resolver)
        val useCase = StreamToolUseCase(resolver, runner)

        val events = mutableListOf<StreamEvent>()
        val nonExistent = if (System.getProperty("os.name").lowercase().contains("win")) {
            "C:\\lti_non_existent_dir_xyz_123"
        } else {
            "/tmp/lti_non_existent_dir_xyz_123"
        }
        val request = ToolExecutionRequest(
            toolId = "echo",
            arguments = listOf("hello"),
            workingDirectory = nonExistent
        )

        useCase.stream(request) { events.add(it) }

        val finished = events.filterIsInstance<StreamEvent.ExecutionFinished>().firstOrNull()
        assertTrue(finished != null, "Must emit ExecutionFinished")
        assertEquals(-1, finished.exitCode)
        assertEquals("working directory missing", finished.summary)
    }

    @Test
    fun `execute rejects null or invalid working directory without spawn`() = runBlocking {
        val resolver = EchoToolResolver()
        val runner = LinuxProcessRunner(resolver)
        val useCase = ExecuteToolUseCase(resolver, runner)

        val nullResp = useCase.execute(
            ToolExecutionRequest(toolId = "echo", arguments = emptyList(), workingDirectory = null)
        )
        assertEquals(-1, nullResp.exitCode)
        assertEquals("working directory missing", nullResp.stderr)

        val relResp = useCase.execute(
            ToolExecutionRequest(toolId = "echo", arguments = emptyList(), workingDirectory = "some/rel/path")
        )
        assertEquals(-1, relResp.exitCode)
        assertEquals("working directory missing", relResp.stderr)
    }

    @Test
    fun `LinuxProcessRunner rejects non-absolute or non-existent working directory`() = runBlocking {
        val resolver = EchoToolResolver()
        val runner = LinuxProcessRunner(resolver)

        val ex = assertFailsWith<IllegalArgumentException> {
            runner.runProcess(
                toolName = "echo",
                arguments = emptyList(),
                workingDir = File("relative/path")
            )
        }
        assertEquals("working directory missing", ex.message)
    }
}
