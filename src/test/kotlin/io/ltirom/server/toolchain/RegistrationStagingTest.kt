package io.ltirom.server.toolchain

import java.io.File
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class RegistrationStagingTest {

    private lateinit var tempDir: File
    private lateinit var toolsDir: File
    private lateinit var stateDir: File
    private lateinit var pluginsDir: File

    @BeforeTest
    fun setUp() {
        tempDir = Files.createTempDirectory("staging-test").toFile()
        toolsDir = File(tempDir, "LtiRomTools").apply { mkdirs() }
        stateDir = File(tempDir, ".ltirom").apply { mkdirs() }
        pluginsDir = File(stateDir, "tools.d").apply { mkdirs() }
    }

    @AfterTest
    fun tearDown() {
        tempDir.deleteRecursively()
    }

    @Test
    fun `when activation is blocked by active work existing registrations remain completely unchanged`() {
        val originalManifest = """{"name":"fastboot","executable":"/old/path/fastboot","owner":"ltirom-toolchain"}"""
        val fastbootFile = File(pluginsDir, "fastboot.json").apply { writeText(originalManifest) }

        val journal = ToolchainJournal(stateDir)
        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal,
            tryAddHold = { listOf("active-run-1") } // busy!
        )

        // Attempt activation of target with different fastboot path
        val outcome = activator.activate(
            requestId = "req-blocked",
            expectedActive = null,
            targetInstallId = "i-new"
        )

        val blocked = assertIs<ActivationOutcome.Blocked>(outcome)
        assertEquals(listOf("active-run-1"), blocked.activeWork)

        // Existing registration must be unmodified
        assertEquals(originalManifest, fastbootFile.readText())
    }
}
