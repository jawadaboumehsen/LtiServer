package io.ltirom.server.engine

import io.ltirom.server.domain.ports.ProcessCancellationPort
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
import kotlin.test.assertTrue

/** A started process whose identity cannot be saved must not be left running where no restart can find it. */
class ProcessIdentityPersistenceTest {
    private lateinit var dir: File
    private lateinit var sessions: ExecutionSessionManager
    private val processes = mutableListOf<Process>()
    private val isWin = System.getProperty("os.name").lowercase().contains("win")

    @BeforeTest
    fun setUp() {
        dir = Files.createTempDirectory("identity-persistence").toFile()
        sessions = ExecutionSessionManager(journalStore = RunJournalStore(baseDir = dir))
    }

    @AfterTest
    fun tearDown() {
        processes.forEach { p ->
            p.descendants().forEach { it.destroyForcibly() }
            p.destroyForcibly()
        }
        dir.deleteRecursively()
    }

    private fun longProcess(): Process = ProcessBuilder(
        if (isWin) listOf("cmd", "/c", "ping -n 60 127.0.0.1 >nul") else listOf("sleep", "60"),
    ).redirectErrorStream(true).start().also { processes += it }

    private class Resolver : ToolResolverPort {
        override fun resolve(toolName: String): File? = File("/fake/$toolName")

        override fun listAvailableTools(): Map<String, Boolean> = mapOf("sleep" to true)
    }

    /** Announces [process] after the journal of the run stopped accepting writes; [obeysKill] decides if it can be stopped. */
    private inner class Runner(private val process: Process, private val obeysKill: Boolean) :
        ProcessExecutionPort,
        ProcessCancellationPort {
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
            File(dir, executionId!!).deleteRecursively()
            File(dir, executionId).writeText("not a folder") // every later write for this run fails
            emit(ProcessOutputChunk.Started(process.pid(), null))
            awaitCancellation()
        }

        override fun cancelProcess(executionId: String, signal: String): Boolean {
            if (obeysKill) {
                process.descendants().forEach { it.destroyForcibly() }
                process.destroyForcibly().waitFor()
            }
            return true
        }

        override fun getSurvivors(executionId: String): List<Long> = if (process.isAlive) listOf(process.pid()) else emptyList()
    }

    private fun start(runner: Runner): String {
        val engine = DefaultServerEngine(Resolver(), runner, "Ubuntu-Test", sessionManager = sessions)
        val request = StartRunRequest(ToolExecutionRequest("sleep", emptyList(), workingDirectory = dir.absolutePath), "k")
        return assertIs<RunStartOutcome.Created>(runBlocking { engine.startRun(request) }).handle.runId
    }

    private fun finishedSummary(runId: String): String? =
        sessions.getRun(runId)!!.session.replay(1L).map { it.event }.filterIsInstance<StreamEvent.ExecutionFinished>()
            .firstOrNull()?.summary

    private fun waitFailed(runId: String) = runBlocking {
        withTimeout(20_000) { while (sessions.getRun(runId)?.status != RunStatusValue.FAILED) delay(50) }
    }

    @Test
    fun `a process whose identity cannot be saved is stopped and the run fails`() {
        val process = longProcess()
        val runId = start(Runner(process, obeysKill = true))

        waitFailed(runId)

        assertFalse(process.isAlive, "the unrecorded process must not keep running")
        val summary = finishedSummary(runId).orEmpty()
        assertTrue("process identity" in summary && "was stopped" in summary, summary)
    }

    @Test
    fun `a process that cannot be confirmed stopped is reported as such`() {
        val process = longProcess()
        val runId = start(Runner(process, obeysKill = false))

        waitFailed(runId)

        val summary = finishedSummary(runId).orEmpty()
        assertTrue("could not be confirmed stopped" in summary && process.pid().toString() in summary, summary)
        assertEquals(RunStatusValue.FAILED, sessions.getRun(runId)!!.status)
    }
}
