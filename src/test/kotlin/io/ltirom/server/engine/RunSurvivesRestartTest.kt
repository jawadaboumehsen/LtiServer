package io.ltirom.server.engine

import io.ltirom.server.core.LinuxProcessRunner
import io.ltirom.server.domain.model.ProcessResourceLimits
import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.domain.ports.ProcessOutputChunk
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.infra.RunJournalStore
import io.ltirom.server.model.RunStartOutcome
import io.ltirom.server.model.RunStatusValue
import io.ltirom.server.model.StartRunRequest
import io.ltirom.server.model.StreamEvent
import io.ltirom.server.model.ToolExecutionRequest
import io.ltirom.server.session.ExecutionSessionManager
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** A run whose process outlives the service is found again after a restart, not declared dead. */
class RunSurvivesRestartTest {
    private lateinit var dir: File
    private val children = mutableListOf<Process>()
    private val isWin = System.getProperty("os.name").lowercase().contains("win")

    @BeforeTest
    fun setUp() {
        dir = Files.createTempDirectory("restart-identity").toFile()
    }

    @AfterTest
    fun tearDown() {
        children.forEach { it.destroyForcibly() }
        dir.deleteRecursively()
    }

    private fun longRunningChild(): Process =
        ProcessBuilder(if (isWin) listOf("ping", "-n", "30", "127.0.0.1") else listOf("sleep", "30"))
            .redirectErrorStream(true).start().also { children += it }

    private fun startOf(process: Process): Long = process.toHandle().info().startInstant().get().toEpochMilli()

    private fun request(key: String) = StartRunRequest(
        ToolExecutionRequest("sleep", emptyList(), workingDirectory = dir.absolutePath),
        key,
    )

    private class Resolver : ToolResolverPort {
        override fun resolve(toolName: String): File? = File("/fake/$toolName")

        override fun listAvailableTools(): Map<String, Boolean> = mapOf("sleep" to true)
    }

    /** A runner whose process is [pid]/[startedAt]; it never ends on its own. */
    private class AnnouncingRunner(val pid: Long, val startedAt: Long?) : ProcessExecutionPort {
        override suspend fun runProcess(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?,
            executionId: String?,
        ): ProcessExecutionResult = awaitCancellation()

        override fun runProcessFlow(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            executionId: String?,
        ): Flow<ProcessOutputChunk> = flow {
            emit(ProcessOutputChunk.Started(pid, startedAt))
            awaitCancellation()
        }
    }

    private fun newEngine(runner: ProcessExecutionPort = AnnouncingRunner(0, null)): DefaultServerEngine =
        DefaultServerEngine(
            Resolver(),
            runner,
            "Ubuntu-Test",
            sessionManager = ExecutionSessionManager(journalStore = RunJournalStore(dir), autoRehydrate = true),
        )

    private suspend fun waitFor(what: String, condition: () -> Boolean) = withTimeout(15_000) {
        while (!condition()) delay(50)
    }.also { check(condition()) { what } }

    @Test
    fun `the runner announces the process before any output`() = runBlocking<Unit> {
        val shell = if (isWin) File("C:\\Windows\\System32\\cmd.exe") else File("/bin/echo")
        val resolver = object : ToolResolverPort {
            override fun resolve(toolName: String) = shell.takeIf { it.exists() }

            override fun listAvailableTools() = mapOf("fast" to true)
        }
        val runner = LinuxProcessRunner(resolver, ProcessResourceLimits(isolatedProcessGroup = false))

        val chunks =
            runner.runProcessFlow("fast", if (isWin) listOf("/c", "echo", "hi") else listOf("hi"), File(".").absoluteFile, emptyMap(), null, 10_000L, null)
                .toList()

        val started = assertIs<ProcessOutputChunk.Started>(chunks.first())
        assertTrue(started.pid > 0 && started.startedAtEpochMs != null)
        assertTrue(chunks.last() is ProcessOutputChunk.Exit)
    }

    @Test
    fun `a started run is saved as running with the identity of its process`() = runBlocking<Unit> {
        val child = longRunningChild()
        val engine = newEngine(AnnouncingRunner(child.pid(), startOf(child)))

        val created = assertIs<RunStartOutcome.Created>(engine.startRun(request("k1")))
        val journal = RunJournalStore(dir)
        waitFor("pid saved") { journal.loadRun(created.handle.runId)?.pid != null }

        val saved = assertNotNull(journal.loadRun(created.handle.runId))
        assertEquals(RunStatusValue.RUNNING, saved.status)
        assertEquals(child.pid(), saved.pid)
        assertEquals(startOf(child), saved.pidStartedAtEpochMs)
    }

    @Test
    fun `after a restart a surviving run stays active and its end is reported when it comes`() = runBlocking<Unit> {
        val child = longRunningChild()
        val first = newEngine(AnnouncingRunner(child.pid(), startOf(child)))
        val runId = assertIs<RunStartOutcome.Created>(first.startRun(request("k2"))).handle.runId
        waitFor("pid saved") { RunJournalStore(dir).loadRun(runId)?.pid != null }

        val restarted = newEngine() // a new service reads the journal
        val run = assertNotNull(restarted.getRun(runId))
        assertEquals(RunStatusValue.RUNNING, run.status, "alive process: not declared dead")
        assertTrue(runId in restarted.activeWork(), "still active work, so an update waits for it")

        child.destroyForcibly()
        waitFor("run settled") { restarted.getRun(runId)?.status == RunStatusValue.INTERRUPTED }
        val events = restarted.attachRun(runId, 1L)!!.toList().map { it.event }
        val finished = events.filterIsInstance<StreamEvent.ExecutionFinished>().single()
        assertEquals(-1, finished.exitCode)
        assertTrue("restarted" in finished.summary.orEmpty(), finished.summary)
        assertTrue(runId !in restarted.activeWork())
    }

    @Test
    fun `a process that is not the recorded one is not adopted, even with the same pid`() = runBlocking<Unit> {
        val child = longRunningChild() // alive, but started long after the run says its process did
        val first = newEngine(AnnouncingRunner(child.pid(), startOf(child) - 3_600_000L))
        val runId = assertIs<RunStartOutcome.Created>(first.startRun(request("k3"))).handle.runId
        waitFor("pid saved") { RunJournalStore(dir).loadRun(runId)?.pid != null }

        val restarted = newEngine()

        assertEquals(RunStatusValue.INTERRUPTED, restarted.getRun(runId)!!.status)
        assertTrue(child.isAlive, "an unrelated process is left alone")
    }

    @Test
    fun `a surviving run can be cancelled after the restart`() = runBlocking<Unit> {
        val child = longRunningChild()
        val first = newEngine(AnnouncingRunner(child.pid(), startOf(child)))
        val runId = assertIs<RunStartOutcome.Created>(first.startRun(request("k4"))).handle.runId
        waitFor("pid saved") { RunJournalStore(dir).loadRun(runId)?.pid != null }
        val restarted = newEngine()

        val response = restarted.cancelRun(runId, "SIGTERM")

        assertTrue(response.accepted)
        waitFor("cancelled") { restarted.getRun(runId)?.status == RunStatusValue.CANCELLED }
        waitFor("process stopped") { !child.isAlive }
    }
}
