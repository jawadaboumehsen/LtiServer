package io.ltirom.server.core

import io.ltirom.server.domain.model.ProcessResourceLimits
import io.ltirom.server.domain.ports.ToolResolverPort
import java.io.File
import java.util.concurrent.TimeUnit
import io.ltirom.server.domain.ports.ProcessExecutionResult
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.flow.buffer
import kotlin.test.*

class ProcessRunnerCancellationTest {
    private val shell = File("/bin/sh")
    private val resolver = object : ToolResolverPort {
        override fun resolve(toolName: String): File? = shell.takeIf { it.canExecute() }
        override fun listAvailableTools(): Map<String, Boolean> = mapOf("sh" to true)
    }

    @Test
    fun `normal exit with background child leaves child tracked as survivor and cancellable in production mode`() = runBlocking {
        org.junit.Assume.assumeTrue("Requires Linux /bin/sh", shell.canExecute())
        val runner = LinuxProcessRunner(resolver, ProcessResourceLimits())
        val ready = CompletableDeferred<Long>()
        val result = runner.runProcess(
            "sh",
            listOf("-c", "sleep 60 & echo $!"),
            File(".").absoluteFile,
            timeoutMs = 5_000,
            executionId = "bg-child",
            onOutput = { out, _ ->
                val trimmed = out.trim()
                if (trimmed.toLongOrNull() != null) ready.complete(trimmed.toLong())
            }
        )
        val childPid = withTimeout(5_000) { ready.await() }
        try {
            assertTrue(ProcessHandle.of(childPid).map { it.isAlive }.orElse(false), "Child should be alive")
            assertTrue(result.survivors.contains(childPid), "Result survivors must contain background child")
            assertTrue(runner.getSurvivors("bg-child").contains(childPid), "getSurvivors must contain background child")
            assertTrue(runner.cancelProcess("bg-child"), "cancelProcess must kill surviving child")
            assertFalse(ProcessHandle.of(childPid).map { it.isAlive }.orElse(false), "Child should be dead after cancel")
        } finally {
            ProcessHandle.of(childPid).ifPresent { if (it.isAlive) it.destroyForcibly() }
        }
    }

    @Test
    fun `unknown cancellation is false and does not poison a later execution`() = runBlocking {
        org.junit.Assume.assumeTrue("Requires Linux /bin/sh", shell.canExecute())
        val runner = LinuxProcessRunner(resolver, ProcessResourceLimits(isolatedProcessGroup = false))
        assertFalse(runner.cancelProcess("missing"))
        val result = runner.runProcess("sh", listOf("-c", "echo ok"), File(".").absoluteFile,
            timeoutMs = 0, executionId = "missing")
        assertEquals(0, result.exitCode)
        assertEquals("ok", result.stdout)
        assertFalse(runner.cancelProcess("missing"))
    }

    @Test
    fun `acknowledgement waits for long running parent and child including TERM resistance`() = runBlocking {
        org.junit.Assume.assumeTrue("Requires Linux /bin/sh", shell.canExecute())
        val runner = LinuxProcessRunner(resolver, ProcessResourceLimits(isolatedProcessGroup = false))
        val ready = CompletableDeferred<List<Long>>()
        val execution = async(Dispatchers.IO) {
            runner.runProcess("sh", listOf("-c",
                "trap '' TERM; sh -c 'trap \"\" TERM; exec sleep 60' & echo \"\$\$ \$!\"; wait"),
                File(".").absoluteFile, timeoutMs = 0, executionId = "long-running",
                onOutput = { out, _ ->
                    if (out.isNotBlank()) ready.complete(out.trim().split(' ').map(String::toLong))
                })
        }
        try {
            val pids = withTimeout(5_000) { ready.await() }
            assertTrue(pids.all { ProcessHandle.of(it).map { p -> p.isAlive }.orElse(false) })
            assertFalse(runner.cancelProcess("long-running", "not-a-signal"))
            val start = System.nanoTime()
            val acknowledged = withContext(Dispatchers.IO) { runner.cancelProcess("long-running") }
            val elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
            assertTrue(elapsed < 4_000, "Cancellation must have a bounded observation window")
            if (acknowledged) {
                assertTrue(pids.none { ProcessHandle.of(it).map { p -> p.isAlive }.orElse(false) },
                    "Success cannot be returned while original or descendant is alive")
            }
            // A conservative false is allowed (for example an unreaped orphan on some hosts);
            // only true carries the terminal guarantee, not false's later liveness snapshot.
        } finally {
            execution.cancelAndJoin()
        }
    }

    @Test
    fun `long running original without descendants receives terminal acknowledgement`() = runBlocking {
        org.junit.Assume.assumeTrue("Requires Linux /bin/sh", shell.canExecute())
        val runner = LinuxProcessRunner(resolver, ProcessResourceLimits(isolatedProcessGroup = false))
        val ready = CompletableDeferred<Long>()
        val execution = async(Dispatchers.IO) {
            runner.runProcess("sh", listOf("-c", "echo \$\$; exec sleep 60"), File(".").absoluteFile,
                timeoutMs = 0, executionId = "original",
                onOutput = { out, _ -> if (out.isNotBlank()) ready.complete(out.toLong()) })
        }
        try {
            val pid = withTimeout(5_000) { ready.await() }
            assertTrue(withContext(Dispatchers.IO) { runner.cancelProcess("original") })
            assertFalse(ProcessHandle.of(pid).map { it.isAlive }.orElse(false))
        } finally {
            execution.cancelAndJoin()
        }
    }

    private fun marked(marker: String) = ProcessHandle.allProcesses()
        .filter { it.isAlive && it.info().commandLine().map { c -> marker in c }.orElse(false) }
        .toList()

    // Nobody receives from the flow, so the Started event is still being delivered when the collector goes away.
    @Test
    fun `cancelled while the Started event is being delivered the spawned command is still stopped`() = runBlocking {
        org.junit.Assume.assumeTrue("Requires Linux /bin/sh", shell.canExecute())
        val marker = "sleep 63.417"
        val started = production().runProcessFlow(
            "sh", listOf("-c", "exec $marker"), File(".").absoluteFile, emptyMap(), null, 0, "no-receiver",
        ).buffer(Channel.RENDEZVOUS).produceIn(this)
        try {
            withTimeout(5_000) { while (marked(marker).isEmpty()) delay(5) }
            started.cancel()
            withTimeout(5_000) { while (marked(marker).isNotEmpty()) delay(20) }
        } finally {
            marked(marker).forEach { it.destroyForcibly() }
        }
    }

    /** Production limits: one session per run, so the group outlives its first process. */
    private fun production() = LinuxProcessRunner(resolver, ProcessResourceLimits())

    private fun alive(pid: Long) = ProcessHandle.of(pid).map { it.isAlive }.orElse(false)

    private fun leaderless(script: String, id: String, runner: LinuxProcessRunner): Pair<Long, ProcessExecutionResult> = runBlocking {
        val ready = CompletableDeferred<Long>()
        val result = runner.runProcess(
            "sh", listOf("-c", script), File(".").absoluteFile, timeoutMs = 5_000, executionId = id,
            onOutput = { out, _ -> out.trim().toLongOrNull()?.let(ready::complete) },
        )
        withTimeout(5_000) { ready.await() } to result
    }

    @Test
    fun `isolated group - a child of a parent that already exited is tracked and cancellation stops it`() {
        org.junit.Assume.assumeTrue("Requires Linux /bin/sh and setsid", shell.canExecute())
        val runner = production()
        val (child, result) = leaderless("sleep 60 & echo \$!", "group-exit", runner)
        try {
            assertTrue(alive(child), "the child outlives its parent")
            assertTrue(result.survivors.contains(child), "survivors: ${result.survivors}")
            assertTrue(runner.getSurvivors("group-exit").contains(child))
            assertTrue(runner.cancelProcess("group-exit"), "cancellation must confirm the whole group is gone")
            assertFalse(alive(child))
        } finally {
            ProcessHandle.of(child).ifPresent { it.destroyForcibly() }
        }
    }

    @Test
    fun `isolated group - a failed command leaves no child behind`() {
        org.junit.Assume.assumeTrue("Requires Linux /bin/sh and setsid", shell.canExecute())
        val (child, result) = leaderless("sleep 60 & echo \$!; exit 3", "group-failed", production())
        try {
            assertEquals(3, result.exitCode)
            assertFalse(alive(child), "a failed run's strays are killed")
        } finally {
            ProcessHandle.of(child).ifPresent { it.destroyForcibly() }
        }
    }
}
