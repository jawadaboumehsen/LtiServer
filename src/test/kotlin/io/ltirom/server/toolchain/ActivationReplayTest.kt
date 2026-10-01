package io.ltirom.server.toolchain

import java.io.File
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class ActivationReplayTest {

    private lateinit var tempDir: File
    private lateinit var toolsDir: File
    private lateinit var stateDir: File

    @BeforeTest
    fun setUp() {
        tempDir = Files.createTempDirectory("replay-test").toFile()
        toolsDir = File(tempDir, "LtiRomTools").apply { mkdirs() }
        stateDir = File(tempDir, ".ltirom").apply { mkdirs() }
    }

    @AfterTest
    fun tearDown() {
        tempDir.deleteRecursively()
    }

    @Test
    fun `computes canonical request hash according to spec`() {
        // request hash = sha256(UTF-8 "v1\n" + (expected ?: "-") + "\n" + (target ?: "-"))
        val hashBothNull = ToolchainActivator.computeRequestHash(null, null)
        val expectedBothNull = ToolchainActivator.sha256("v1\n-\n-")
        assertEquals(expectedBothNull, hashBothNull)

        val hashWithExpected = ToolchainActivator.computeRequestHash("i-old", "i-new")
        val expectedWithVals = ToolchainActivator.sha256("v1\ni-old\ni-new")
        assertEquals(expectedWithVals, hashWithExpected)
    }

    @Test
    fun `same id and same hash returns stored outcome even if BLOCKED`() {
        val journal = ToolchainJournal(stateDir)
        val reqHash = ToolchainActivator.computeRequestHash("legacy", "i-target")
        journal.append(
            requestId = "req-blocked",
            type = JournalEntryType.REJECTED,
            payload = """{"code":"BLOCKED","reason":"busy","requestHash":"$reqHash"}"""
        )

        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal
        )

        val outcome = activator.activate(
            requestId = "req-blocked",
            expectedActive = "legacy",
            targetInstallId = "i-target"
        )

        val rejected = assertIs<ActivationOutcome.Rejected>(outcome)
        assertEquals("BLOCKED", rejected.code)
    }

    @Test
    fun `same id and different hash returns REQUEST_MISMATCH`() {
        val journal = ToolchainJournal(stateDir)
        val reqHash = ToolchainActivator.computeRequestHash("legacy", "i-first")
        journal.append(
            requestId = "req-1",
            type = JournalEntryType.COMMITTED,
            payload = """{"activeInstallId":"i-first","requestHash":"$reqHash"}"""
        )

        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal
        )

        // Same ID, different target -> different hash
        val outcome = activator.activate(
            requestId = "req-1",
            expectedActive = "legacy",
            targetInstallId = "i-different"
        )

        val rejected = assertIs<ActivationOutcome.Rejected>(outcome)
        assertEquals("REQUEST_MISMATCH", rejected.code)
    }

    @Test
    fun `replay while in SWITCHING state returns IN_PROGRESS`() {
        val journal = ToolchainJournal(stateDir)
        val reqHash = ToolchainActivator.computeRequestHash("legacy", "i-switching")
        journal.append(
            requestId = "req-sw",
            type = JournalEntryType.REQUEST,
            payload = """{"from":"legacy","to":"i-switching","requestHash":"$reqHash"}"""
        )

        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal
        )

        val outcome = activator.activate(
            requestId = "req-sw",
            expectedActive = "legacy",
            targetInstallId = "i-switching"
        )

        assertIs<ActivationOutcome.InProgress>(outcome)
    }

    @Test
    fun `stale expected install id returns CONFLICT`() {
        val journal = ToolchainJournal(stateDir)
        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal,
            activeInstallProvider = { "i-actual-active" }
        )

        val outcome = activator.activate(
            requestId = "req-conflict",
            expectedActive = "i-stale-expected",
            targetInstallId = "i-target"
        )

        val rejected = assertIs<ActivationOutcome.Rejected>(outcome)
        assertEquals("CONFLICT", rejected.code)
        assertEquals("i-actual-active", rejected.actualActiveInstallId)
    }
}
