package io.ltirom.server.infra

import io.ltirom.server.model.PersistedRunMetadata
import io.ltirom.server.model.RunStatusValue
import io.ltirom.server.model.SequencedStreamEvent
import io.ltirom.server.model.StartRunRequest
import io.ltirom.server.model.StreamEvent
import io.ltirom.server.model.ToolExecutionRequest
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RunJournalDurabilityTest {
    private lateinit var dir: File
    private lateinit var store: RunJournalStore

    @BeforeTest
    fun setUp() {
        dir = Files.createTempDirectory("journal-durable").toFile()
        store = RunJournalStore(baseDir = dir, isPidAlive = { false })
    }

    @AfterTest
    fun tearDown() {
        dir.deleteRecursively()
    }

    private fun metadata(id: String) = PersistedRunMetadata(
        runId = id,
        request = StartRunRequest(ToolExecutionRequest("sh", listOf("-c", "true"), workingDirectory = "/"), "k-$id", null),
        workingDirectory = "/",
        workspaceLock = null,
        idempotencyKey = "k-$id",
        status = RunStatusValue.RUNNING,
        startedAtEpochMs = 1L,
    )

    @Test
    fun `every update replaces the record whole and leaves no temp file`() {
        store.recordRun(metadata("r1"))
        repeat(50) { seq ->
            store.appendEvent("r1", SequencedStreamEvent(seq + 1L, StreamEvent.OutputChunk("line $seq")))
        }

        assertEquals(50L, store.loadRun("r1")!!.lastSeq)
        assertTrue(File(dir, "r1").listFiles()!!.none { it.name.endsWith(".tmp") })
    }

    @Test
    fun `a temp file left by a crash does not hide the last complete record`() {
        store.recordRun(metadata("r2"))
        File(dir, "r2/run.json.tmp").writeText("{\"runId\":\"r2\",\"stat") // died mid-write

        assertNotNull(store.loadRun("r2"))
        store.updateRunMetadata("r2") { it.copy(status = RunStatusValue.COMPLETED) }
        assertEquals(RunStatusValue.COMPLETED, store.loadRun("r2")!!.status)
    }

    @Test
    fun `a damaged record is reported by name instead of looking like a run that never existed`() {
        store.recordRun(metadata("r3"))
        File(dir, "r3/run.json").writeText("{\"runId\":\"r3\",\"stat")
        val captured = ByteArrayOutputStream()
        val original = System.err
        System.setErr(PrintStream(captured))
        try {
            assertNull(store.loadRun("r3"))
            assertTrue(store.loadAllRuns().isEmpty())
        } finally {
            System.setErr(original)
        }

        assertTrue("r3" in captured.toString() && "unreadable" in captured.toString(), captured.toString())
    }
}
