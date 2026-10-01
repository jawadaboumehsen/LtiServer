package io.ltirom.server.session

import io.ltirom.server.infra.RunJournalStore
import io.ltirom.server.model.*
import java.io.File
import java.nio.file.Files
import kotlin.test.*

class RunJournalTest {

    private lateinit var tempDir: File
    private lateinit var journalStore: RunJournalStore
    private val deadPidChecker: (Long) -> Boolean = { false }
    private val alivePidChecker: (Long) -> Boolean = { pid -> pid == 12345L }

    @BeforeTest
    fun setUp() {
        tempDir = Files.createTempDirectory("ltirom-journal-test").toFile()
        journalStore = RunJournalStore(baseDir = tempDir, isPidAlive = deadPidChecker)
    }

    @AfterTest
    fun tearDown() {
        tempDir.deleteRecursively()
    }

    @Test
    fun `events appended to events_jsonl with monotonic seq`() {
        val manager = ExecutionSessionManager(journalStore = journalStore)
        val request = StartRunRequest(
            request = ToolExecutionRequest(
                toolId = "mkfs.erofs",
                arguments = listOf("-z", "lz4hc", "out.img", "work/"),
                workingDirectory = tempDir.absolutePath
            ),
            idempotencyKey = "run-mono:1",
            workspaceLock = tempDir.absolutePath
        )
        val run = manager.registerRun("run-mono", request, "run-mono:1", tempDir.absolutePath)

        val e1 = run.session.appendEvent(StreamEvent.OutputChunk("Step 1 starting"))
        val e2 = run.session.appendEvent(StreamEvent.OutputChunk("Step 1 progress"))
        val e3 = run.session.appendEvent(StreamEvent.ExecutionFinished(exitCode = 0, durationMs = 1500L, summary = "done"))

        assertEquals(1L, e1.seq)
        assertEquals(2L, e2.seq)
        assertEquals(3L, e3.seq)

        val eventsFile = File(tempDir, "run-mono/events.jsonl")
        assertTrue(eventsFile.exists(), "events.jsonl must exist on disk")
        val lines = eventsFile.readLines().filter { it.isNotBlank() }
        assertEquals(3, lines.size)

        val replayed = journalStore.replay("run-mono", fromSeq = 1L)
        assertEquals(3, replayed.size)
        assertEquals(1L, replayed[0].seq)
        assertEquals(2L, replayed[1].seq)
        assertEquals(3L, replayed[2].seq)
    }

    @Test
    fun `replay from file below ring floor`() {
        // Create session with small buffer capacity of 3
        val manager = ExecutionSessionManager(defaultCapacity = 3, journalStore = journalStore)
        val request = StartRunRequest(
            request = ToolExecutionRequest(toolId = "echo", arguments = listOf("hello"), workingDirectory = tempDir.absolutePath),
            idempotencyKey = "run-floor:1"
        )
        val run = manager.registerRun("run-floor", request, "run-floor:1", null)

        for (i in 1..6) {
            run.session.appendEvent(StreamEvent.OutputChunk("line $i"))
        }

        // Buffer capacity is 3, so in-memory ring only contains events 4, 5, 6
        val ringOnly = run.session.ringBuffer.replay(fromSeq = 1L)
        assertEquals(3, ringOnly.size)
        assertEquals(4L, ringOnly.first().seq)
        assertEquals(6L, ringOnly.last().seq)

        // Session-level replay should fall back to file journal when fromSeq is below ring floor
        val fullReplay = run.session.replay(fromSeq = 2L)
        assertEquals(5, fullReplay.size)
        assertEquals(2L, fullReplay.first().seq)
        assertEquals(6L, fullReplay.last().seq)
    }

    @Test
    fun `more than 10000 events lose nothing`() {
        val manager = ExecutionSessionManager(defaultCapacity = 10_000, journalStore = journalStore)
        val request = StartRunRequest(
            request = ToolExecutionRequest(toolId = "cat", arguments = listOf("large.txt"), workingDirectory = tempDir.absolutePath),
            idempotencyKey = "run-large:1"
        )
        val run = manager.registerRun("run-large", request, "run-large:1", null)

        val totalEvents = 10_500
        for (i in 1..totalEvents) {
            run.session.appendEvent(StreamEvent.OutputChunk("log entry $i"))
        }

        // The ring buffer dropped the first 500 events
        assertEquals(10_000, run.session.ringBuffer.replay(1L).size)

        // The journal-backed replay recovers all 10,500 events
        val replayed = run.session.replay(fromSeq = 1L)
        assertEquals(totalEvents, replayed.size)
        assertEquals(1L, replayed.first().seq)
        assertEquals(totalEvents.toLong(), replayed.last().seq)
    }

    @Test
    fun `rehydrate marks RUNNING with dead pid INTERRUPTED`() {
        val runId1 = "run-dead-pid"
        val runId2 = "run-alive-pid"
        val runId3 = "run-completed"

        val req = StartRunRequest(
            request = ToolExecutionRequest(toolId = "mkfs", arguments = emptyList(), workingDirectory = tempDir.absolutePath),
            idempotencyKey = "key-rehydrate"
        )

        val meta1 = PersistedRunMetadata(
            runId = runId1,
            request = req,
            workingDirectory = tempDir.absolutePath,
            idempotencyKey = "k1",
            status = RunStatusValue.RUNNING,
            pid = 99999L,
            startedAtEpochMs = System.currentTimeMillis() - 10000
        )
        val meta2 = PersistedRunMetadata(
            runId = runId2,
            request = req,
            workingDirectory = tempDir.absolutePath,
            idempotencyKey = "k2",
            status = RunStatusValue.RUNNING,
            pid = 12345L,
            startedAtEpochMs = System.currentTimeMillis() - 5000
        )
        val meta3 = PersistedRunMetadata(
            runId = runId3,
            request = req,
            workingDirectory = tempDir.absolutePath,
            idempotencyKey = "k3",
            status = RunStatusValue.COMPLETED,
            exitCode = 0,
            startedAtEpochMs = System.currentTimeMillis() - 20000,
            endedAtEpochMs = System.currentTimeMillis() - 15000
        )

        journalStore.recordRun(meta1)
        journalStore.recordRun(meta2)
        journalStore.recordRun(meta3)

        // Create a new store with the pid checker that knows 12345 is alive, others dead
        val recoveryStore = RunJournalStore(baseDir = tempDir, isPidAlive = alivePidChecker)
        val recoveredManager = ExecutionSessionManager(journalStore = recoveryStore)
        recoveredManager.rehydrate()

        val r1 = recoveredManager.getRun(runId1)
        assertNotNull(r1)
        assertEquals(RunStatusValue.INTERRUPTED, r1.status)

        val r2 = recoveredManager.getRun(runId2)
        assertNotNull(r2)
        assertEquals(RunStatusValue.RUNNING, r2.status)

        val r3 = recoveredManager.getRun(runId3)
        assertNotNull(r3)
        assertEquals(RunStatusValue.COMPLETED, r3.status)
    }

    @Test
    fun `prune terminal runs older than 30 days`() {
        val now = System.currentTimeMillis()
        val thirtyOneDaysAgo = now - (31L * 24 * 3600 * 1000L)
        val fiveDaysAgo = now - (5L * 24 * 3600 * 1000L)

        val req = StartRunRequest(
            request = ToolExecutionRequest(toolId = "ls", arguments = emptyList(), workingDirectory = tempDir.absolutePath),
            idempotencyKey = "key-prune"
        )

        // Old terminal run -> should be pruned
        val oldCompleted = PersistedRunMetadata(
            runId = "run-old-comp",
            request = req,
            workingDirectory = tempDir.absolutePath,
            idempotencyKey = "k-old",
            status = RunStatusValue.COMPLETED,
            startedAtEpochMs = thirtyOneDaysAgo - 1000,
            endedAtEpochMs = thirtyOneDaysAgo
        )
        journalStore.recordRun(oldCompleted)

        // Recent terminal run -> kept
        val recentCompleted = PersistedRunMetadata(
            runId = "run-recent-comp",
            request = req,
            workingDirectory = tempDir.absolutePath,
            idempotencyKey = "k-rec",
            status = RunStatusValue.COMPLETED,
            startedAtEpochMs = fiveDaysAgo - 1000,
            endedAtEpochMs = fiveDaysAgo
        )
        journalStore.recordRun(recentCompleted)

        // Old running/active run -> kept (not terminal)
        val oldRunning = PersistedRunMetadata(
            runId = "run-old-running",
            request = req,
            workingDirectory = tempDir.absolutePath,
            idempotencyKey = "k-run",
            status = RunStatusValue.RUNNING,
            startedAtEpochMs = thirtyOneDaysAgo
        )
        journalStore.recordRun(oldRunning)

        val prunedCount = journalStore.pruneOldRuns(now = now)
        assertEquals(1, prunedCount)

        assertNull(journalStore.loadRun("run-old-comp"))
        assertNotNull(journalStore.loadRun("run-recent-comp"))
        assertNotNull(journalStore.loadRun("run-old-running"))
    }
}
