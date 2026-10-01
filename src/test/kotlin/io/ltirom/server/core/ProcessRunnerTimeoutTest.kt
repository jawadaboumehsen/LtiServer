package io.ltirom.server.core

import io.ltirom.server.domain.model.ProcessResourceLimits
import io.ltirom.server.domain.ports.ProcessOutputChunk
import io.ltirom.server.domain.ports.ToolResolverPort
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ProcessRunnerTimeoutTest {

    private class SleepyToolResolver : ToolResolverPort {
        override fun resolve(toolName: String): File? {
            val isWin = System.getProperty("os.name").lowercase().contains("win")
            return when (toolName) {
                "fast" -> {
                    val p = if (isWin) File("C:\\Windows\\System32\\cmd.exe") else File("/bin/echo")
                    if (p.exists()) p else File(".")
                }
                "slow" -> {
                    val p = if (isWin) File("C:\\Windows\\System32\\PING.EXE") else File("/bin/sleep")
                    if (p.exists()) p else File(".")
                }
                else -> null
            }
        }

        override fun listAvailableTools(): Map<String, Boolean> = mapOf("fast" to true, "slow" to true)
    }

    private val isWin = System.getProperty("os.name").lowercase().contains("win")
    private val fastArgs = if (isWin) listOf("/c", "echo", "done") else listOf("done")
    private val slowArgs = if (isWin) listOf("-n", "3", "127.0.0.1") else listOf("3")
    private val workingDir = File(".").absoluteFile

    @Test
    fun `limits default and negative or zero timeout`() {
        val defaultLimits = ProcessResourceLimits()
        assertTrue(defaultLimits.hasDeadline)
        assertEquals(900_000L, defaultLimits.timeoutMs)

        val noDeadlineLimits = ProcessResourceLimits(timeoutMs = 0L)
        assertFalse(noDeadlineLimits.hasDeadline)

        val negativeLimits = ProcessResourceLimits(timeoutMs = -1L)
        assertFalse(negativeLimits.hasDeadline)
    }

    @Test
    fun `runProcess with timeoutMs zero executes without deadline`() = runBlocking {
        val resolver = SleepyToolResolver()
        val runner = LinuxProcessRunner(resolver, ProcessResourceLimits(isolatedProcessGroup = false))

        val res = runner.runProcess(
            toolName = "fast",
            arguments = fastArgs,
            workingDir = workingDir,
            timeoutMs = 0L
        )
        assertEquals(0, res.exitCode)
        assertTrue(res.stdout.contains("done"))
    }

    @Test
    fun `runProcess with negative timeout executes without deadline`() = runBlocking {
        val resolver = SleepyToolResolver()
        val runner = LinuxProcessRunner(resolver, ProcessResourceLimits(isolatedProcessGroup = false))

        val res = runner.runProcess(
            toolName = "fast",
            arguments = fastArgs,
            workingDir = workingDir,
            timeoutMs = -1L
        )
        assertEquals(0, res.exitCode)
        assertTrue(res.stdout.contains("done"))
    }

    @Test
    fun `runProcess with positive timeout times out when slow`() = runBlocking {
        val resolver = SleepyToolResolver()
        val runner = LinuxProcessRunner(resolver, ProcessResourceLimits(isolatedProcessGroup = false))

        val ex = assertFailsWith<IllegalStateException> {
            runner.runProcess(
                toolName = "slow",
                arguments = slowArgs,
                workingDir = workingDir,
                timeoutMs = 50L
            )
        }
        assertTrue(ex.message!!.contains("timed out after 50 ms"))
    }

    @Test
    fun `runProcessFlow with timeoutMs zero executes without deadline`() = runBlocking {
        val resolver = SleepyToolResolver()
        val runner = LinuxProcessRunner(resolver, ProcessResourceLimits(isolatedProcessGroup = false))

        val chunks = runner.runProcessFlow(
            toolName = "fast",
            arguments = fastArgs,
            workingDir = workingDir,
            timeoutMs = 0L
        ).toList()

        val exitChunk = chunks.filterIsInstance<ProcessOutputChunk.Exit>().firstOrNull()
        assertTrue(exitChunk != null, "Must have Exit chunk")
        assertEquals(0, exitChunk.exitCode)
    }

    @Test
    fun `runProcessFlow with positive timeout times out when slow`() = runBlocking {
        val resolver = SleepyToolResolver()
        val runner = LinuxProcessRunner(resolver, ProcessResourceLimits(isolatedProcessGroup = false))

        val ex = assertFailsWith<IllegalStateException> {
            runner.runProcessFlow(
                toolName = "slow",
                arguments = slowArgs,
                workingDir = workingDir,
                timeoutMs = 50L
            ).toList()
        }
        assertTrue(ex.message!!.contains("timed out after 50 ms"))
    }
}
