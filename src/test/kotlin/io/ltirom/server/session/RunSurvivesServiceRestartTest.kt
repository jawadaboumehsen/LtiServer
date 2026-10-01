package io.ltirom.server.session

import io.ltirom.server.infra.RunJournalStore
import io.ltirom.server.model.PersistedRunMetadata
import io.ltirom.server.model.RunStatusValue
import io.ltirom.server.model.SequencedStreamEvent
import io.ltirom.server.model.StartRunRequest
import io.ltirom.server.model.StreamEvent
import io.ltirom.server.model.ToolExecutionRequest
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import io.ltirom.server.engine.DefaultServerEngine
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.core.LinuxProcessRunner
import kotlin.io.path.createTempDirectory
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * A client asked a replacement service about a run the previous service had finished and got "run not
 * found", although the outcome was journaled on disk. A new service reloads the journal.
 */
class RunSurvivesServiceRestartTest {

    private val dir = createTempDirectory("ltirom-runs").toFile()

    @AfterTest
    fun cleanup() {
        dir.deleteRecursively()
    }

    private fun meta(runId: String, status: RunStatusValue, lastSeq: Long = 0L) = PersistedRunMetadata(
        runId = runId,
        request = StartRunRequest(ToolExecutionRequest(toolId = "cmake", arguments = emptyList()), idempotencyKey = "k-$runId"),
        workingDirectory = "/tmp",
        idempotencyKey = "k-$runId",
        status = status,
        exitCode = if (status == RunStatusValue.COMPLETED) 0 else null,
        startedAtEpochMs = 1L,
        lastSeq = lastSeq,
    )

    @Test
    fun `a new service reports and replays a run the previous service finished`() = runBlocking {
        // Written by the previous service.
        val previous = RunJournalStore(baseDir = dir)
        previous.recordRun(meta("r1", RunStatusValue.COMPLETED, lastSeq = 2))
        previous.appendEvent("r1", SequencedStreamEvent(1, StreamEvent.OutputChunk("-- Configuring done")))
        previous.appendEvent("r1", SequencedStreamEvent(2, StreamEvent.ExecutionFinished(0, 2610L)))

        val replacement = ExecutionSessionManager(journalStore = RunJournalStore(baseDir = dir), autoRehydrate = true)

        val run = assertNotNull(replacement.getRun("r1"), "the finished run must not be 'not found'")
        assertEquals(RunStatusValue.COMPLETED, run.status)
        assertEquals(0, run.exitCode)
        val replayed = run.session.attach(fromSeq = 1).toList()
        assertTrue(replayed.last().event is StreamEvent.ExecutionFinished, "$replayed")
        assertTrue(replacement.activeRuns().isEmpty())
    }

    @Test
    fun `a run a dead service left queued is interrupted, not active forever`() {
        RunJournalStore(baseDir = dir).recordRun(meta("r2", RunStatusValue.QUEUED))

        val replacement = ExecutionSessionManager(
            journalStore = RunJournalStore(baseDir = dir, isPidAlive = { false }),
            autoRehydrate = true,
        )

        assertEquals(RunStatusValue.INTERRUPTED, replacement.getRun("r2")?.status)
        assertTrue(replacement.activeRuns().isEmpty(), "it must not block shutdown or the workspace lock")
    }

    @Test
    fun `attaching to a finished run past its last event ends instead of waiting forever`() = runBlocking {
        val previous = RunJournalStore(baseDir = dir)
        previous.recordRun(meta("r3", RunStatusValue.COMPLETED, lastSeq = 2))
        previous.appendEvent("r3", SequencedStreamEvent(1, StreamEvent.OutputChunk("out")))
        previous.appendEvent("r3", SequencedStreamEvent(2, StreamEvent.ExecutionFinished(0, 1L)))
        val resolver = object : ToolResolverPort {
            override fun resolve(toolName: String): java.io.File? = null
            override fun listAvailableTools(): Map<String, Boolean> = emptyMap()
        }
        val engine = DefaultServerEngine(
            toolResolver = resolver,
            processRunner = LinuxProcessRunner(resolver),
            distroName = "Ubuntu",
            sessionManager = ExecutionSessionManager(journalStore = RunJournalStore(baseDir = dir), autoRehydrate = true),
        )

        val all = withTimeout(5_000) { assertNotNull(engine.attachRun("r3", 1)).toList() }
        val afterEnd = withTimeout(5_000) { assertNotNull(engine.attachRun("r3", 3)).toList() }

        assertEquals(listOf(1L, 2L), all.map { it.seq })
        assertTrue(afterEnd.isEmpty(), "nothing left to replay: the stream ends")
    }
}
