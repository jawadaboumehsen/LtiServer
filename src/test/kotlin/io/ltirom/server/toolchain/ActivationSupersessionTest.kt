package io.ltirom.server.toolchain

import java.io.File
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class ActivationSupersessionTest {

    private lateinit var tempDir: File
    private lateinit var toolsDir: File
    private lateinit var stateDir: File

    @BeforeTest
    fun setUp() {
        tempDir = Files.createTempDirectory("supersession-test").toFile()
        toolsDir = File(tempDir, "LtiRomTools").apply { mkdirs() }
        stateDir = File(tempDir, ".ltirom").apply { mkdirs() }
    }

    @AfterTest
    fun tearDown() {
        tempDir.deleteRecursively()
    }

    @Test
    fun `MAINTENANCE_FAILED is resolved by later committed activation C and restart preserves C`() {
        val journal = ToolchainJournal(stateDir)

        // 1. Transaction A failed with MAINTENANCE_FAILED
        val reqHashA = ToolchainActivator.computeRequestHash("legacy", "i-failed-A")
        journal.append(
            requestId = "req-A",
            type = JournalEntryType.REQUEST,
            payload = """{"from":"legacy","to":"i-failed-A","requestHash":"$reqHashA"}"""
        )
        journal.append(
            requestId = "req-A",
            type = JournalEntryType.MAINTENANCE_FAILED,
            payload = """{"reason":"Disk corruption"}"""
        )

        var fold = journal.fold()
        assertEquals(ToolchainState.RECOVERY_REQUIRED, fold.state)
        assertEquals("req-A", fold.recoveryRequiredBy)

        // 2. Later activation C succeeds and COMMITS
        val reqHashC = ToolchainActivator.computeRequestHash("legacy", "i-clean-C")
        journal.append(
            requestId = "req-C",
            type = JournalEntryType.REQUEST,
            payload = """{"from":"legacy","to":"i-clean-C","requestHash":"$reqHashC"}"""
        )
        journal.append(
            requestId = "req-C",
            type = JournalEntryType.COMMITTED,
            payload = """{"activeInstallId":"i-clean-C"}"""
        )

        fold = journal.fold()
        assertEquals(ToolchainState.READY, fold.state)
        assertEquals("i-clean-C", fold.activeInstallId)
        assertNull(fold.recoveryRequiredBy, "Superseded failure must clear recoveryRequiredBy")

        val recordA = fold.activations["req-A"]
        assertNotNull(recordA)
        assertEquals("req-C", recordA.resolvedBy)

        // 3. Restart / new journal instance: C stays active, recovery does not run for req-A
        val newJournal = ToolchainJournal(stateDir)
        val restartFold = newJournal.fold()
        assertEquals(ToolchainState.READY, restartFold.state)
        assertEquals("i-clean-C", restartFold.activeInstallId)
        assertNull(restartFold.recoveryRequiredBy)
    }
}
