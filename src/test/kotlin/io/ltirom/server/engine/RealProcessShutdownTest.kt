package io.ltirom.server.engine

import io.ltirom.server.core.LinuxProcessRunner
import io.ltirom.server.domain.model.ProcessResourceLimits
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.model.RunStartOutcome
import io.ltirom.server.model.StartRunRequest
import io.ltirom.server.model.ToolExecutionRequest
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** Real processes, production limits: stopping the service stops what a finished command left running. */
class RealProcessShutdownTest {
    private val shell = File("/bin/sh")
    private val resolver = object : ToolResolverPort {
        override fun resolve(toolName: String): File? = shell.takeIf { it.canExecute() }

        override fun listAvailableTools(): Map<String, Boolean> = mapOf("sh" to true)
    }

    private fun marked(marker: String) = ProcessHandle.allProcesses().filter {
        it.isAlive && it.info().commandLine().map { c -> marker in c }.orElse(false)
    }.toList()

    @Test
    fun `a command that exited but left a child is active work and a forced stop kills the child`() = runBlocking<Unit> {
        org.junit.Assume.assumeTrue("Requires Linux /bin/sh and setsid", shell.canExecute())
        val marker = "sleep 61.${System.nanoTime() % 100000}"
        val engine = DefaultServerEngine(resolver, LinuxProcessRunner(resolver, ProcessResourceLimits()), "Ubuntu-Test")
        val request = ToolExecutionRequest("sh", listOf("-c", "$marker & echo started"), File(".").absolutePath)
        val runId = assertIs<RunStartOutcome.Created>(engine.startRun(StartRunRequest(request, "orphan"))).handle.runId
        try {
            withTimeout(10_000) { while (marked(marker).isEmpty()) delay(20) }
            withTimeout(10_000) { while (runId !in engine.activeWork()) delay(20) }

            val active = engine.closeAdmission(force = true)
            assertTrue(runId in active, "the run that left a child is still active work: $active")
            val remaining = engine.cancelWorkAndWait(active, waitMs = 7_000)

            assertEquals(emptyList(), remaining)
            val left = marked(marker)
            assertTrue(left.isEmpty(), "the child must be gone: " + left.map { File("/proc/${it.pid()}/stat").readText() })
        } finally {
            marked(marker).forEach { it.destroyForcibly() }
        }
    }

    @Test
    fun `cancelling right as a command exits never leaves its child behind`() = runBlocking<Unit> {
        org.junit.Assume.assumeTrue("Requires Linux /bin/sh and setsid", shell.canExecute())
        val marker = "sleep 62.${System.nanoTime() % 100000}"
        val engine = DefaultServerEngine(resolver, LinuxProcessRunner(resolver, ProcessResourceLimits()), "Ubuntu-Test")
        val request = ToolExecutionRequest("sh", listOf("-c", "$marker & echo started"), File(".").absolutePath)
        try {
            repeat(40) { i ->
                val runId = assertIs<RunStartOutcome.Created>(engine.startRun(StartRunRequest(request, "race-$i"))).handle.runId
                // The command takes a few milliseconds; this stop lands before, during and after its exit.
                delay(i % 8L)
                val remaining = engine.cancelWorkAndWait(listOf(runId), waitMs = 7_000)
                assertEquals(emptyList(), remaining, "iteration $i")
            }
            val left = marked(marker)
            assertTrue(left.isEmpty(), "children left behind: " + left.map { it.pid() })
        } finally {
            marked(marker).forEach { it.destroyForcibly() }
        }
    }
}
