package io.ltirom.server.session

import io.ltirom.server.infra.RunJournalStore
import io.ltirom.server.model.RunStatusValue
import io.ltirom.server.model.StartRunRequest
import io.ltirom.server.model.StreamEvent
import io.ltirom.server.model.ToolExecutionRequest
import java.io.File
import java.nio.file.Files
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SessionManagerResilienceTest {
    private lateinit var dir: File
    private lateinit var manager: ExecutionSessionManager

    @BeforeTest
    fun setUp() {
        dir = Files.createTempDirectory("session-resilience").toFile()
        manager = ExecutionSessionManager(journalStore = RunJournalStore(baseDir = dir, isPidAlive = { false }))
    }

    @AfterTest
    fun tearDown() {
        dir.deleteRecursively()
    }

    private fun request(key: String) =
        StartRunRequest(ToolExecutionRequest("sh", listOf("-c", "true"), workingDirectory = dir.absolutePath), key, null)

    /** Makes every write for [runId] fail: a file sits where its folder has to be. */
    private fun breakJournalOf(runId: String) {
        File(dir, runId).deleteRecursively()
        File(dir, runId).writeText("not a folder")
    }

    @Test
    fun `a run that could not be saved leaves nothing behind`() {
        breakJournalOf("r-lost")

        assertFailsWith<java.io.IOException> { manager.registerRun("r-lost", request("k-lost"), "k-lost", null) }

        assertNull(manager.getRun("r-lost"))
        assertTrue(manager.activeRuns().isEmpty(), "no queued run without a worker")
        assertNull(manager.findIdempotentRun("k-lost"), "a retry with the same key starts fresh")
    }

    @Test
    fun `a failed write does not stop a finished run from being reported`() {
        val run = manager.registerRun("r-done", request("k-done"), "k-done", null)
        breakJournalOf("r-done")

        manager.updateRunStatus("r-done", RunStatusValue.COMPLETED, exitCode = 0, endedAtEpochMs = 5L)
        val finished = run.session.appendEvent(StreamEvent.ExecutionFinished(0, 5L))

        assertEquals(RunStatusValue.COMPLETED, manager.getRun("r-done")!!.status)
        assertTrue(run.session.replay(1L).any { it.seq == finished.seq && it.event is StreamEvent.ExecutionFinished })
    }

    @Test
    fun `attaching after the journal stopped writing still reaches the retained output and the finish`() = runBlocking {
        val small = ExecutionSessionManager(
            defaultCapacity = 5,
            journalStore = RunJournalStore(baseDir = dir, isPidAlive = { false }),
        )
        val run = small.registerRun("r-gap", request("k-gap"), "k-gap", null)
        breakJournalOf("r-gap")
        repeat(20) { run.session.appendEvent(StreamEvent.OutputChunk("line $it")) }
        run.session.appendEvent(StreamEvent.ExecutionFinished(0, 5L))

        val seen = withTimeout(5_000) { run.session.attach(0L).toList() }

        assertTrue(seen.last().event is StreamEvent.ExecutionFinished, "the finish must reach the client")
        assertTrue(seen.first().seq > 1L, "the lost history is skipped, not invented")
        assertEquals(seen.first().seq, run.session.replayPage(0L).skippedToSeq, "the gap is reported")
        assertTrue(run.session.replay(0L).any { it.event is StreamEvent.ExecutionFinished })
    }

    @Test
    fun `old finished runs and expired keys are forgotten, active work never`() {
        val hour = 3_600_000L
        val active = manager.registerRun("active", request("k-active"), "k-active", null)
        manager.updateRunStatus("active", RunStatusValue.RUNNING)
        repeat(205) { i ->
            manager.registerRun("old-$i", request("k-$i"), "k-$i", null)
            manager.updateRunStatus("old-$i", RunStatusValue.COMPLETED, exitCode = 0, endedAtEpochMs = 1_000L + i)
        }

        manager.pruneFinished(now = System.currentTimeMillis() + 3 * hour)

        assertNotNull(manager.getRun("active"), "active work is kept")
        assertNull(manager.listRuns().firstOrNull { it.runId == "old-0" }, "old and beyond the newest 200")
        assertNotNull(manager.listRuns().firstOrNull { it.runId == "old-204" }, "the newest are kept")
        assertNotNull(manager.getRun("old-0"), "but a client asking for it still gets it, from the journal")
        assertEquals(200, manager.listRuns().count { it.status == RunStatusValue.COMPLETED })
        assertNull(manager.findIdempotentRun("k-0", ttlMs = Long.MAX_VALUE), "expired keys are evicted")
        assertTrue(active.runId in manager.activeRuns().map { it.runId })
    }

    @Test
    fun `recent finished runs are kept even beyond the count`() {
        val now = System.currentTimeMillis()
        repeat(205) { i ->
            manager.registerRun("r-$i", request("k-$i"), "k-$i", null)
            manager.updateRunStatus("r-$i", RunStatusValue.COMPLETED, exitCode = 0, endedAtEpochMs = now)
        }

        manager.pruneFinished(now = now + 1_000L)

        assertEquals(205, manager.listRuns().size, "a client may still be reading these")
    }

    @Test
    fun `a run dropped from memory is restored from the journal with its output`() {
        val run = manager.registerRun("r-old", request("k-old"), "k-old", null)
        run.session.appendEvent(StreamEvent.OutputChunk("[ 50%] Building"))
        run.session.appendEvent(StreamEvent.ExecutionFinished(0, 5L))
        manager.updateRunStatus("r-old", RunStatusValue.COMPLETED, exitCode = 0, endedAtEpochMs = 5L)
        repeat(201) { i ->
            manager.registerRun("filler-$i", request("kf-$i"), "kf-$i", null)
            manager.updateRunStatus("filler-$i", RunStatusValue.COMPLETED, exitCode = 0, endedAtEpochMs = 10L + i)
        }
        manager.pruneFinished(now = System.currentTimeMillis() + 2 * 3_600_000L)
        assertNull(manager.listRuns().firstOrNull { it.runId == "r-old" }, "dropped from memory")

        val restored = assertNotNull(manager.getRun("r-old"))

        assertEquals(RunStatusValue.COMPLETED, restored.status)
        val events = restored.session.replay(1L)
        assertEquals(listOf(1L, 2L), events.map { it.seq })
        assertTrue(events.last().event is StreamEvent.ExecutionFinished)
    }

    @Test
    fun `an unfinished journal record and an unsafe id are not restored`() {
        manager.registerRun("r-live", request("k-live"), "k-live", null) // journal says QUEUED
        val unknownToMemory = ExecutionSessionManager(journalStore = RunJournalStore(baseDir = dir, isPidAlive = { false }))

        assertNull(unknownToMemory.getRun("r-live"), "an active-looking record belongs to a dead service")
        assertNull(unknownToMemory.getRun("../r-live"))
        assertNull(unknownToMemory.getRun("..\r-live"))
    }
}
