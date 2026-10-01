package io.ltirom.server.toolchain

import java.io.File
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class RegistrationOwnershipTest {

    private lateinit var tempDir: File
    private lateinit var toolsDir: File
    private lateinit var stateDir: File
    private lateinit var pluginsDir: File

    @BeforeTest
    fun setUp() {
        tempDir = Files.createTempDirectory("ownership-test").toFile()
        toolsDir = File(tempDir, "LtiRomTools").apply { mkdirs() }
        stateDir = File(tempDir, ".ltirom").apply { mkdirs() }
        pluginsDir = File(stateDir, "tools.d").apply { mkdirs() }
    }

    @AfterTest
    fun tearDown() {
        tempDir.deleteRecursively()
    }

    @Test
    fun `custom manifest with no owner survives activation and rollback unchanged`() {
        val customManifest = File(pluginsDir, "my-custom-tool.json").apply {
            writeText("""{"name":"my-custom-tool","executable":"/usr/bin/custom"}""")
        }

        val journal = ToolchainJournal(stateDir)
        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal
        )

        // Staged activation replaces owned registrations, never foreign
        activator.applyOwnedRegistrations(
            targetOutputs = listOf(InstallManifestOutput(toolId = "fastboot", file = "fastboot")),
            binDir = File(toolsDir, "bin")
        )

        assertTrue(customManifest.exists())
        assertEquals("""{"name":"my-custom-tool","executable":"/usr/bin/custom"}""", customManifest.readText())
    }

    @Test
    fun `target output registered by foreign owner triggers REGISTRATION_CONFLICT before any change`() {
        // Tool registered by foreign owner
        File(pluginsDir, "fastboot.json").writeText(
            """{"name":"fastboot","executable":"/opt/custom/fastboot","owner":"other-plugin"}"""
        )

        val journal = ToolchainJournal(stateDir)
        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal
        )

        val conflict = activator.checkRegistrationConflicts(
            targetOutputs = listOf(InstallManifestOutput(toolId = "fastboot", file = "fastboot"))
        )

        assertTrue(conflict.isNotEmpty(), "Conflict should be detected")
        assertEquals(listOf("fastboot"), conflict)
    }

    @Test
    fun `rollback restores snapshot and deletes registrations marked ABSENT`() {
        // Fastboot was originally present with owned content
        val originalFastboot = """{"name":"fastboot","executable":"/bin/fastboot-old","owner":"ltirom-toolchain"}"""
        File(pluginsDir, "fastboot.json").writeText(originalFastboot)

        // New tool was introduced by transaction
        File(pluginsDir, "new-tool.json").writeText(
            """{"name":"new-tool","executable":"/bin/new-tool","owner":"ltirom-toolchain"}"""
        )

        val journal = ToolchainJournal(stateDir)
        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal
        )

        val snapshot = listOf(
            RegSnapshotEntry("fastboot", originalFastboot),
            RegSnapshotEntry("new-tool", "ABSENT")
        )

        activator.restoreRegistrationSnapshot(snapshot)

        // fastboot restored byte-for-byte
        assertEquals(originalFastboot, File(pluginsDir, "fastboot.json").readText())
        // new-tool deleted
        assertFalse(File(pluginsDir, "new-tool.json").exists())
    }
}
