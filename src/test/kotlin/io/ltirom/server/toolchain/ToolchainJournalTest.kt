package io.ltirom.server.toolchain

import java.io.File
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ToolchainJournalTest {

    private lateinit var tempDir: File
    private lateinit var journalFile: File
    private lateinit var stateDir: File

    @BeforeTest
    fun setUp() {
        tempDir = Files.createTempDirectory("journal-test").toFile()
        stateDir = File(tempDir, ".ltirom").apply { mkdirs() }
        journalFile = File(stateDir, "toolchain-journal.jsonl")
    }

    @AfterTest
    fun tearDown() {
        tempDir.deleteRecursively()
    }

    private fun createJournal() = ToolchainJournal(stateDir)

    @Test
    fun `empty journal yields UNINITIALIZED state with no active install`() {
        val journal = createJournal()
        val fold = journal.fold()

        assertEquals(ToolchainState.UNINITIALIZED, fold.state)
        assertNull(fold.activeInstallId)
        assertNull(fold.previousInstallId)
        assertNull(fold.recoveryRequiredBy)
        assertNull(fold.inFlightTransaction)
    }

    @Test
    fun `rebuilds projections identically from journal if projection files are deleted or stale`() {
        val journal = createJournal()
        journal.append(
            requestId = "req-1",
            type = JournalEntryType.REQUEST,
            payload = """{"from":null,"to":"i-first","requestHash":"hash1"}"""
        )
        journal.append(
            requestId = "req-1",
            type = JournalEntryType.COMMITTED,
            payload = """{"activeInstallId":"i-first"}"""
        )

        val stateFile = File(stateDir, "toolchain-state.json")
        val activationFile = File(stateDir, "activations/req-1.json")
        assertTrue(stateFile.exists())
        assertTrue(activationFile.exists())

        // Delete projection files
        stateFile.delete()
        activationFile.delete()

        // Folding again rebuilds projections identically
        val fold = journal.fold()
        assertEquals(ToolchainState.READY, fold.state)
        assertEquals("i-first", fold.activeInstallId)

        assertTrue(stateFile.exists(), "toolchain-state.json must be rebuilt")
        assertTrue(activationFile.exists(), "activation projection must be rebuilt")
    }

    @Test
    fun `discards torn last line with invalid JSON or CRC mismatch`() {
        val journal = createJournal()
        journal.append(
            requestId = "req-1",
            type = JournalEntryType.REQUEST,
            payload = """{"from":null,"to":"i-1","requestHash":"h1"}"""
        )
        journal.append(
            requestId = "req-1",
            type = JournalEntryType.COMMITTED,
            payload = """{"activeInstallId":"i-1"}"""
        )

        // Append a torn/corrupted last line
        journalFile.appendText("{\"seq\":3,\"requestId\":\"req-2\",\"type\":\"REQUEST\",\"crc32\":99999\n")

        val fold = journal.fold()
        assertEquals(ToolchainState.READY, fold.state)
        assertEquals("i-1", fold.activeInstallId)
        assertNull(fold.inFlightTransaction, "Torn entry must be ignored")
    }

    @Test
    fun `identifies in flight transaction when journal ends at intent entries`() {
        val journal = createJournal()

        // 1. REQUEST only
        journal.append(
            requestId = "req-1",
            type = JournalEntryType.REQUEST,
            payload = """{"from":null,"to":"i-target","requestHash":"hash-req"}"""
        )
        var fold = journal.fold()
        var inFlight = assertNotNull(fold.inFlightTransaction)
        assertEquals("req-1", inFlight.requestId)
        assertEquals(JournalEntryType.REQUEST, inFlight.lastIntentType)

        // 2. REG_SNAPSHOT
        journal.append(
            requestId = "req-1",
            type = JournalEntryType.REG_SNAPSHOT,
            payload = """[{"toolId":"fastboot","previous":"ABSENT"}]"""
        )
        fold = journal.fold()
        inFlight = assertNotNull(fold.inFlightTransaction)
        assertEquals(JournalEntryType.REG_SNAPSHOT, inFlight.lastIntentType)

        // 3. LINK_INTENT
        journal.append(
            requestId = "req-1",
            type = JournalEntryType.LINK_INTENT,
            payload = """{"target":"i-target"}"""
        )
        fold = journal.fold()
        inFlight = assertNotNull(fold.inFlightTransaction)
        assertEquals(JournalEntryType.LINK_INTENT, inFlight.lastIntentType)

        // 4. RESTORE_STARTED
        journal.append(
            requestId = "req-1",
            type = JournalEntryType.RESTORE_STARTED,
            payload = """{"target":null,"reason":"verify failed"}"""
        )
        fold = journal.fold()
        inFlight = assertNotNull(fold.inFlightTransaction)
        assertEquals(JournalEntryType.RESTORE_STARTED, inFlight.lastIntentType)
    }

    @Test
    fun `completion entry terminates in flight transaction`() {
        val journal = createJournal()
        journal.append(
            requestId = "req-1",
            type = JournalEntryType.REQUEST,
            payload = """{"from":null,"to":"i-target","requestHash":"hash1"}"""
        )
        journal.append(
            requestId = "req-1",
            type = JournalEntryType.COMMITTED,
            payload = """{"activeInstallId":"i-target"}"""
        )

        val fold = journal.fold()
        assertNull(fold.inFlightTransaction, "Committed transaction is not in-flight")
        assertEquals(ToolchainState.READY, fold.state)
        assertEquals("i-target", fold.activeInstallId)
    }

    @Test
    fun `MAINTENANCE_FAILED sets RECOVERY_REQUIRED and is resolved by RESTORED of same transaction`() {
        val journal = createJournal()
        journal.append(
            requestId = "req-fail",
            type = JournalEntryType.REQUEST,
            payload = """{"from":"i-old","to":"i-broken","requestHash":"h-fail"}"""
        )
        journal.append(
            requestId = "req-fail",
            type = JournalEntryType.MAINTENANCE_FAILED,
            payload = """{"reason":"Cannot restore link"}"""
        )

        var fold = journal.fold()
        assertEquals(ToolchainState.RECOVERY_REQUIRED, fold.state)
        assertEquals("req-fail", fold.recoveryRequiredBy)

        // Successful crash recovery of req-fail appends RESTORED for req-fail
        journal.append(
            requestId = "req-fail",
            type = JournalEntryType.RESTORED,
            payload = """{"activeInstallId":"i-old"}"""
        )

        fold = journal.fold()
        assertEquals(ToolchainState.READY, fold.state)
        assertNull(fold.recoveryRequiredBy, "recoveryRequiredBy must be cleared after RESTORED")
        assertEquals("i-old", fold.activeInstallId)
    }

    @Test
    fun `MAINTENANCE_FAILED is superseded by a later COMMITTED transaction`() {
        val journal = createJournal()
        journal.append(
            requestId = "req-old-fail",
            type = JournalEntryType.REQUEST,
            payload = """{"from":"i-1","to":"i-2","requestHash":"h1"}"""
        )
        journal.append(
            requestId = "req-old-fail",
            type = JournalEntryType.MAINTENANCE_FAILED,
            payload = """{"reason":"broken"}"""
        )

        var fold = journal.fold()
        assertEquals(ToolchainState.RECOVERY_REQUIRED, fold.state)
        assertEquals("req-old-fail", fold.recoveryRequiredBy)

        // A new activation req-new commits
        journal.append(
            requestId = "req-new",
            type = JournalEntryType.REQUEST,
            payload = """{"from":"i-1","to":"i-3","requestHash":"h2"}"""
        )
        journal.append(
            requestId = "req-new",
            type = JournalEntryType.COMMITTED,
            payload = """{"activeInstallId":"i-3"}"""
        )

        fold = journal.fold()
        assertEquals(ToolchainState.READY, fold.state)
        assertNull(fold.recoveryRequiredBy, "Superseded by later COMMITTED")
        assertEquals("i-3", fold.activeInstallId)
        assertEquals("i-1", fold.previousInstallId)

        val oldRecord = fold.activations["req-old-fail"]
        assertNotNull(oldRecord)
        assertEquals("req-new", oldRecord.resolvedBy, "old record tracks resolvedBy")
    }

    @Test
    fun `a later COMMITTED whose projection write was lost still clears derived recoveryRequiredBy on restart`() {
        val journal = createJournal()
        journal.append(
            requestId = "req-fail",
            type = JournalEntryType.REQUEST,
            payload = """{"from":"i-1","to":"i-2","requestHash":"h1"}"""
        )
        journal.append(
            requestId = "req-fail",
            type = JournalEntryType.MAINTENANCE_FAILED,
            payload = """{"reason":"broken"}"""
        )

        // Simulate crash right after journal append of COMMITTED before projections rewrite
        journal.append(
            requestId = "req-c",
            type = JournalEntryType.REQUEST,
            payload = """{"from":"i-1","to":"i-clean","requestHash":"h2"}"""
        )
        journal.append(
            requestId = "req-c",
            type = JournalEntryType.COMMITTED,
            payload = """{"activeInstallId":"i-clean"}"""
        )

        // Stale state file claiming RECOVERY_REQUIRED
        File(stateDir, "toolchain-state.json").writeText(
            """{"state":"RECOVERY_REQUIRED","activeInstallId":"i-1","previousInstallId":null,"recoveryRequiredBy":"req-fail"}"""
        )

        // Folding must derive truth from journal, ignoring the stale projection
        val fold = journal.fold()
        assertEquals(ToolchainState.READY, fold.state)
        assertEquals("i-clean", fold.activeInstallId)
        assertNull(fold.recoveryRequiredBy)
    }
}
