package io.ltirom.server.engine

import io.ltirom.server.core.LinuxProcessRunner
import io.ltirom.server.core.Survivor
import io.ltirom.server.domain.model.ProcessResourceLimits
import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.domain.ports.ProcessOutputChunk
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.infra.RunJournalStore
import io.ltirom.server.model.RunStartOutcome
import io.ltirom.server.model.RunStatusValue
import io.ltirom.server.model.StartRunRequest
import io.ltirom.server.model.ToolExecutionRequest
import io.ltirom.server.session.ExecutionSessionManager
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Real parent/child processes: a run is over when its whole process tree is, and only its own processes are touched. */
class ProcessTreeLifecycleTest {
    private lateinit var dir: File
    private val started = mutableListOf<Process>()
    private val isWin = System.getProperty("os.name").lowercase().contains("win")

    @BeforeTest
    fun setUp() {
        dir = Files.createTempDirectory("process-tree").toFile()
    }

    @AfterTest
    fun tearDown() {
        started.forEach { it.descendants().forEach { child -> child.destroyForcibly() } }
        started.forEach { it.destroyForcibly() }
        dir.deleteRecursively()
    }

    /** A parent that waits on a child: cmd -> ping on Windows, sh -> sleep elsewhere. */
    private fun parentWithChild(marker: String): Process =
        ProcessBuilder(
            if (isWin) {
                listOf("cmd", "/c", "ping -n $marker 127.0.0.1 >nul")
            } else {
                listOf("sh", "-c", "sleep $marker; true")
            },
        ).redirectErrorStream(true).start().also { started += it }

    private fun waitUntil(what: String, timeoutMs: Long = 15_000, condition: () -> Boolean) = runBlocking {
        withTimeout(timeoutMs) { while (!condition()) delay(50) }
    }.also { check(condition()) { what } }

    private class Resolver : ToolResolverPort {
        override fun resolve(toolName: String): File? = File("/fake/$toolName")

        override fun listAvailableTools(): Map<String, Boolean> = mapOf("sleep" to true)
    }

    private class Announcing(val pid: Long, val startedAt: Long?) : ProcessExecutionPort {
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

    private fun engine(runner: ProcessExecutionPort = Announcing(0, null)) = DefaultServerEngine(
        Resolver(),
        runner,
        "Ubuntu-Test",
        sessionManager = ExecutionSessionManager(journalStore = RunJournalStore(dir), autoRehydrate = true),
    )

    private fun request(key: String) =
        StartRunRequest(ToolExecutionRequest("sleep", emptyList(), workingDirectory = dir.absolutePath), key)

    private fun adoptedRun(parent: Process, key: String): Pair<DefaultServerEngine, String> = runBlocking {
        val startedAt = parent.toHandle().info().startInstant().get().toEpochMilli()
        val runId = assertIs<RunStartOutcome.Created>(engine(Announcing(parent.pid(), startedAt)).startRun(request(key)))
            .handle.runId
        waitUntil("pid saved") { RunJournalStore(dir).loadRun(runId)?.pid != null }
        engine() to runId
    }

    private fun childOf(parent: Process): ProcessHandle {
        waitUntil("child started") { parent.descendants().findFirst().isPresent }
        return parent.descendants().findFirst().get()
    }

    @Test
    fun `cancelling an adopted run stops the children as well as the parent`() = runBlocking<Unit> {
        val parent = parentWithChild("40")
        val child = childOf(parent)
        val (restarted, runId) = adoptedRun(parent, "tree-cancel")
        delay(600) // the new service has seen the whole tree

        restarted.cancelRun(runId, "SIGTERM")

        waitUntil("cancelled") { restarted.getRun(runId)?.status == RunStatusValue.CANCELLED }
        assertFalse(child.isAlive, "the child must not outlive its cancelled run")
        assertFalse(parent.isAlive)
    }

    @Test
    fun `a parent that exits while its child lives does not end the run`() = runBlocking<Unit> {
        val parent = parentWithChild("40")
        childOf(parent)
        val kids = parent.descendants().toList() // everything the parent started (a console host, the command itself)
        val (restarted, runId) = adoptedRun(parent, "tree-orphan")
        delay(600)

        parent.destroyForcibly() // only the parent goes; its children carry on
        parent.waitFor()
        delay(800)

        assertTrue(kids.any { it.isAlive }, "the children outlived the parent")
        assertEquals(RunStatusValue.RUNNING, restarted.getRun(runId)!!.status, "they still belong to the run")
        assertTrue(runId in restarted.activeWork())

        kids.forEach { it.destroyForcibly() }
        waitUntil("run settled") { restarted.getRun(runId)?.status == RunStatusValue.INTERRUPTED }
    }

    @Test
    fun `a survivor record names a process by pid and start time, never by pid alone`() {
        val child = parentWithChild("40")
        val realStart = child.toHandle().info().startInstant().get().toEpochMilli()

        assertNotNull(Survivor(child.pid(), realStart).handle(), "the recorded process")
        assertNotNull(Survivor(child.pid(), null).handle(), "no start time recorded: pid only, as before")
        assertNull(
            Survivor(child.pid(), realStart - 3_600_000L).handle(),
            "the same pid with another start time is a different process and is left alone",
        )
        child.destroyForcibly()
        child.waitFor()
        assertNull(Survivor(child.pid(), realStart).handle(), "a gone process has no handle")
    }

    private fun shellFor(exit: Int, marker: String): Pair<File, List<String>> = if (isWin) {
        File("C:\\Windows\\System32\\cmd.exe") to listOf(
            "/c",
            "start /b ping -n $marker 127.0.0.1 >nul 2>&1 & ping -n 3 127.0.0.1 >nul & exit $exit",
        )
    } else {
        File("/bin/sh") to listOf("-c", "sleep $marker >/dev/null 2>&1 & sleep 1; exit $exit")
    }

    /** Live ping/sleep processes: the stand-in children. (Arguments of other processes are not readable on Windows.) */
    private fun standIns(): Map<Long, ProcessHandle> = ProcessHandle.allProcesses().filter { handle ->
        val command = handle.info().command().orElse("").lowercase()
        command.endsWith("ping.exe") || command.endsWith("/sleep") || command.endsWith("\\sleep")
    }.toList().associateBy { it.pid() }

    /** Runs the command and returns its result with the stand-in children it left alive. */
    private fun runShell(exit: Int, marker: String): Pair<ProcessExecutionResult, List<ProcessHandle>> {
        val before = standIns().keys
        val result = runBlocking {
            val (shell, args) = shellFor(exit, marker)
            val resolver = object : ToolResolverPort {
                override fun resolve(toolName: String) = shell.takeIf { it.exists() }

                override fun listAvailableTools() = mapOf("shell" to true)
            }
            LinuxProcessRunner(resolver, ProcessResourceLimits(isolatedProcessGroup = false))
                .runProcess("shell", args, File(".").absoluteFile, emptyMap(), null, 30_000L, null, null)
        }
        return result to standIns().filterKeys { it !in before }.values.toList()
    }

    @Test
    fun `a command that fails leaves no ordinary child process behind`() {
        val (result, leftovers) = runShell(exit = 1, marker = "4711")
        leftovers.forEach { it.destroyForcibly() }

        assertEquals(1, result.exitCode)
        assertTrue(leftovers.isEmpty(), "the child of a failed command was cleaned up, found $leftovers")
        assertTrue(result.survivors.isEmpty(), "nothing is reported as surviving: ${result.survivors}")
    }

    @Test
    fun `a command that succeeds may leave a child on purpose, and it is reported not killed`() {
        val (result, leftovers) = runShell(exit = 0, marker = "4712")
        try {
            assertEquals(0, result.exitCode)
            assertTrue(leftovers.isNotEmpty(), "the child of a successful command is not killed")
            assertTrue(result.survivors.isNotEmpty(), "but it is reported")
        } finally {
            leftovers.forEach { it.destroyForcibly() }
        }
    }

    @Test
    fun `production wrapping starts a new session and lowers the open-file limit, other modes run the command as is`() {
        val wrapped = ProcessResourceLimits(maxOpenFiles = 2048).wrapCommand(listOf("/usr/bin/adb", "devices"))

        assertEquals(listOf("setsid", "sh", "-c"), wrapped.take(3))
        assertTrue("ulimit -n 2048" in wrapped[3] && "exec \"\$@\"" in wrapped[3], wrapped[3])
        assertEquals(listOf("sh", "/usr/bin/adb", "devices"), wrapped.drop(4), "the command follows as arguments")

        assertEquals(
            listOf("/usr/bin/adb", "devices"),
            ProcessResourceLimits(isolatedProcessGroup = false).wrapCommand(listOf("/usr/bin/adb", "devices")),
        )
    }
}
