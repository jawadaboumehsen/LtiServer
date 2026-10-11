package io.ltirom.server.core

import io.ltirom.server.domain.model.ProcessResourceLimits
import io.ltirom.server.domain.ports.ToolResolverPort
import kotlinx.coroutines.runBlocking
import java.io.File
import java.io.StringReader
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ProcessOutputHandlingTest {
    private fun lines(text: String, maxLine: Int = 64 * 1024): List<String> {
        val result = mutableListOf<String>()
        StringReader(text).forEachBoundedLine(maxLine) { result += it }
        return result
    }

    @Test
    fun `lines end the way readLine ends them`() {
        assertEquals(listOf("a", "b", "", "c"), lines("a\nb\n\nc"))
        assertEquals(listOf("a", "b"), lines("a\r\nb\r\n"))
        assertEquals(listOf("10%", "50%", "100%"), lines("10%\r50%\r100%\n"), "progress bars update with \\r")
        assertEquals(emptyList(), lines(""))
    }

    @Test
    fun `a line longer than the limit arrives in pieces instead of all at once`() {
        val pieces = lines("x".repeat(250) + "\nok", maxLine = 100)

        assertEquals(listOf(100, 100, 50, 2), pieces.map { it.length })
        assertTrue(pieces.all { it.length <= 100 })
    }

    @Test
    fun `capture never exceeds its limit and says what was cut`() {
        val capture = BoundedCapture(maxChars = 20)
        repeat(10) { capture.add("0123456789") }

        val text = capture.text()
        assertTrue(text.startsWith("0123456789\n0"), text)
        assertTrue("[output truncated at 20 characters]" in text, text)
        assertTrue(text.length < 80, "only the marker is added to the limit")
    }

    @Test
    fun `what was read is kept when the drain gave up`() {
        val capture = BoundedCapture(maxChars = 1000)
        capture.add("compiled 1")
        capture.add("compiled 2")

        val text = capture.text(drainedFully = false)

        assertTrue(text.startsWith("compiled 1\ncompiled 2"), text)
        assertTrue("end may be missing" in text, text)
    }

    private class StdinResolver : ToolResolverPort {
        private val isWin = System.getProperty("os.name").lowercase().contains("win")

        override fun resolve(toolName: String): File? {
            val tool = if (isWin) File("C:\\Windows\\System32\\sort.exe") else File("/bin/cat")
            return tool.takeIf { toolName == "reader" && it.exists() }
        }

        override fun listAvailableTools(): Map<String, Boolean> = mapOf("reader" to true)
    }

    @Test
    fun `a command that reads stdin to the end gets EOF when no input was given`() = runBlocking {
        val runner = LinuxProcessRunner(StdinResolver(), ProcessResourceLimits(isolatedProcessGroup = false))

        // Used to wait for input that never came, until the timeout killed it.
        val result =
            runner.runProcess(
                toolName = "reader",
                arguments = emptyList(),
                workingDir = File(".").absoluteFile,
                environment = emptyMap(),
                stdinText = null,
                timeoutMs = 20_000L,
                onOutput = null,
                executionId = null,
            )

        assertEquals(0, result.exitCode)
    }
}
