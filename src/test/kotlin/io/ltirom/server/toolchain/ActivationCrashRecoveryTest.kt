package io.ltirom.server.toolchain

import java.io.File
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ActivationCrashRecoveryTest {

    private lateinit var tempDir: File
    private lateinit var toolsDir: File
    private lateinit var stateDir: File

    @BeforeTest
    fun setUp() {
        tempDir = Files.createTempDirectory("crash-rec-test").toFile()
        toolsDir = File(tempDir, "LtiRomTools").apply { mkdirs() }
        stateDir = File(tempDir, ".ltirom").apply { mkdirs() }
    }

    @AfterTest
    fun tearDown() {
        tempDir.deleteRecursively()
    }

    @Test
    fun `crash at LINK_INTENT when bin was already swapped to target converges to COMMITTED on verify pass`() {
        val journal = ToolchainJournal(stateDir)
        val reqHash = ToolchainActivator.computeRequestHash("i-from", "i-to")
        journal.append(
            requestId = "req-crash-1",
            type = JournalEntryType.REQUEST,
            payload = """{"from":"i-from","to":"i-to","requestHash":"$reqHash"}"""
        )
        journal.append(
            requestId = "req-crash-1",
            type = JournalEntryType.REG_SNAPSHOT,
            payload = """[]"""
        )
        journal.append(
            requestId = "req-crash-1",
            type = JournalEntryType.LINK_INTENT,
            payload = """{"target":"i-to"}"""
        )

        // Observed bin is already "i-to"
        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal,
            activeInstallProvider = { "i-to" },
            verifyProvider = { true }
        )

        activator.recoverStartup()

        val fold = journal.fold()
        assertEquals(ToolchainState.READY, fold.state)
        assertEquals("i-to", fold.activeInstallId)
        assertNull(fold.inFlightTransaction)
    }

    @Test
    fun `crash at LINK_INTENT when swap never occurred restores snapshot and sets RESTORED`() {
        val journal = ToolchainJournal(stateDir)
        val reqHash = ToolchainActivator.computeRequestHash("i-from", "i-to")
        journal.append(
            requestId = "req-crash-2",
            type = JournalEntryType.REQUEST,
            payload = """{"from":"i-from","to":"i-to","requestHash":"$reqHash"}"""
        )
        journal.append(
            requestId = "req-crash-2",
            type = JournalEntryType.REG_SNAPSHOT,
            payload = """[]"""
        )
        journal.append(
            requestId = "req-crash-2",
            type = JournalEntryType.LINK_INTENT,
            payload = """{"target":"i-to"}"""
        )

        // Observed bin is still "i-from" (swap never happened before crash)
        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal,
            activeInstallProvider = { "i-from" },
            verifyProvider = { true }
        )

        activator.recoverStartup()

        val fold = journal.fold()
        assertEquals(ToolchainState.READY, fold.state)
        assertEquals("i-from", fold.activeInstallId)
        assertNull(fold.inFlightTransaction)
    }

    @Test
    fun `crash during rollback after link restored before RESTORE_LINKED converges to RESTORED`() {
        val journal = ToolchainJournal(stateDir)
        val reqHash = ToolchainActivator.computeRequestHash("i-from", "i-to")
        journal.append(
            requestId = "req-crash-3",
            type = JournalEntryType.REQUEST,
            payload = """{"from":"i-from","to":"i-to","requestHash":"$reqHash"}"""
        )
        journal.append(
            requestId = "req-crash-3",
            type = JournalEntryType.RESTORE_STARTED,
            payload = """{"target":"i-from","reason":"verify failed"}"""
        )

        // Link is already restored to i-from
        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal,
            activeInstallProvider = { "i-from" },
            verifyProvider = { true }
        )

        activator.recoverStartup()

        val fold = journal.fold()
        assertEquals(ToolchainState.READY, fold.state)
        assertEquals("i-from", fold.activeInstallId)
        assertNull(fold.inFlightTransaction)
    }

    @Test
    fun `observed bin neither from nor to results in RECOVERY_REQUIRED`() {
        val journal = ToolchainJournal(stateDir)
        val reqHash = ToolchainActivator.computeRequestHash("i-from", "i-to")
        journal.append(
            requestId = "req-crash-4",
            type = JournalEntryType.REQUEST,
            payload = """{"from":"i-from","to":"i-to","requestHash":"$reqHash"}"""
        )
        journal.append(
            requestId = "req-crash-4",
            type = JournalEntryType.LINK_INTENT,
            payload = """{"target":"i-to"}"""
        )

        // Observed bin is foreign/corrupted
        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal,
            activeInstallProvider = { "i-foreign-damaged" }
        )

        activator.recoverStartup()

        val fold = journal.fold()
        assertEquals(ToolchainState.RECOVERY_REQUIRED, fold.state)
        assertEquals("req-crash-4", fold.recoveryRequiredBy)
    }
}
