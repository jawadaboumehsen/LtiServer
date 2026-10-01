package io.ltirom.server.toolchain

import java.io.File
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ToolchainActivatorTest {

    private lateinit var tempDir: File
    private lateinit var toolsDir: File
    private lateinit var stateDir: File
    private lateinit var pluginsDir: File
    private lateinit var installsDir: File
    private val simulatedSymlinks = mutableMapOf<File, File>()

    @BeforeTest
    fun setUp() {
        tempDir = Files.createTempDirectory("activator-test").toFile()
        toolsDir = File(tempDir, "LtiRomTools").apply { mkdirs() }
        stateDir = File(tempDir, ".ltirom").apply { mkdirs() }
        pluginsDir = File(stateDir, "tools.d").apply { mkdirs() }
        installsDir = File(toolsDir, "installs").apply { mkdirs() }
        File(toolsDir, "artifacts").apply { mkdirs() }
        simulatedSymlinks.clear()
    }

    @AfterTest
    fun tearDown() {
        tempDir.deleteRecursively()
    }

    private fun createSymlink(link: File, target: File) {
        val created = runCatching {
            Files.createSymbolicLink(link.toPath(), target.toPath())
        }.isSuccess
        if (!created) {
            link.writeText("symlink")
            simulatedSymlinks[link] = target
        }
    }

    private fun mockLinkSwapper(targetInstallId: String?) {
        val binDir = File(toolsDir, "bin")
        if (targetInstallId == null) {
            binDir.deleteRecursively()
            simulatedSymlinks.remove(binDir)
            return
        }
        val targetBin = File(toolsDir, "installs/$targetInstallId/bin")
        binDir.deleteRecursively()
        createSymlink(binDir, targetBin)
    }

    private fun createValidator(): InstallValidator = InstallValidator(
        toolsDir = toolsDir,
        serviceDistro = "Ubuntu-Test",
        serviceArch = "x86_64",
        realPathResolver = { file ->
            val simulated = simulatedSymlinks[file]
            if (simulated != null) {
                runCatching { simulated.toPath().toRealPath().toFile() }.getOrDefault(simulated)
            } else {
                runCatching { file.toPath().toRealPath().toFile() }.getOrNull()
            }
        }
    )

    private fun createInstall(id: String, toolId: String, fileName: String): File {
        val artifactsDir = File(toolsDir, "artifacts").apply { mkdirs() }
        val artDir = File(artifactsDir, "art-$id").apply { mkdirs() }
        val artBin = File(artDir, fileName).apply { writeText("binary content of $toolId") }

        val dir = File(installsDir, id).apply { mkdirs() }
        val bin = File(dir, "bin").apply { mkdirs() }
        val symlinkFile = File(bin, fileName)
        createSymlink(symlinkFile, artBin)

        File(dir, "install.json").writeText(
            """
            {
              "schema": 1,
              "distro": "Ubuntu-Test",
              "arch": "x86_64",
              "outputs": [
                { "toolId": "$toolId", "file": "$fileName" }
              ]
            }
            """.trimIndent()
        )
        return dir
    }

    @Test
    fun `successful activation commits and applies registrations from outputs`() {
        createInstall("i-target", "fastboot", "fastboot")
        val journal = ToolchainJournal(stateDir)

        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal,
            validator = createValidator(),
            activeInstallProvider = { null },
            verifyProvider = { true },
            linkSwapper = ::mockLinkSwapper
        )

        val outcome = activator.activate(
            requestId = "req-succ",
            expectedActive = null,
            targetInstallId = "i-target"
        )

        val committed = assertIs<ActivationOutcome.Committed>(outcome)
        assertEquals("i-target", committed.activeInstallId)

        // Verifies bin link exists
        val binDir = File(toolsDir, "bin")
        assertTrue(binDir.exists())

        // Verifies registration is applied with owner "ltirom-toolchain"
        val fastbootManifest = File(pluginsDir, "fastboot.json")
        assertTrue(fastbootManifest.exists())
        assertTrue(fastbootManifest.readText().contains("\"owner\":\"ltirom-toolchain\""))

        // Journal reflects committed state
        val fold = journal.fold()
        assertEquals(ToolchainState.READY, fold.state)
        assertEquals("i-target", fold.activeInstallId)
    }

    @Test
    fun `first activation verify failure removes bin and returns to UNINITIALIZED`() {
        createInstall("i-first-target", "fastboot", "fastboot")
        val journal = ToolchainJournal(stateDir)

        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal,
            validator = createValidator(),
            activeInstallProvider = { null },
            verifyProvider = { false }, // Verification fails!
            linkSwapper = ::mockLinkSwapper
        )

        val outcome = activator.activate(
            requestId = "req-fail-first",
            expectedActive = null,
            targetInstallId = "i-first-target"
        )

        val verifyFailed = assertIs<ActivationOutcome.VerifyFailedRestored>(outcome)
        assertNull(verifyFailed.activeInstallId)

        // First activation failure removes bin directory
        val binDir = File(toolsDir, "bin")
        assertFalse(binDir.exists(), "bin link must be removed when rolling back first activation")

        // Registrations introduced by transaction removed
        assertFalse(File(pluginsDir, "fastboot.json").exists())

        val fold = journal.fold()
        assertEquals(ToolchainState.UNINITIALIZED, fold.state)
        assertNull(fold.activeInstallId)
    }

    @Test
    fun `verification failure restores link and registrations before holds released`() {
        createInstall("i-initial", "fastboot", "fastboot")
        createInstall("i-candidate", "fastboot", "fastboot")
        val journal = ToolchainJournal(stateDir)

        // Set up initial state with i-initial committed
        val initialManifest = """{"name":"fastboot","executable":"/old/fastboot","owner":"ltirom-toolchain"}"""
        File(pluginsDir, "fastboot.json").writeText(initialManifest)

        var linkSwappedTo: String? = "i-initial"

        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal,
            validator = createValidator(),
            activeInstallProvider = { linkSwappedTo },
            linkSwapper = { target -> linkSwappedTo = target },
            verifyProvider = { target ->
                // i-candidate fails verification, i-initial passes
                target != "i-candidate"
            }
        )

        val outcome = activator.activate(
            requestId = "req-restore",
            expectedActive = "i-initial",
            targetInstallId = "i-candidate"
        )

        val failedRestored = assertIs<ActivationOutcome.VerifyFailedRestored>(outcome)
        assertEquals("i-initial", failedRestored.activeInstallId)

        // Link restored back to i-initial
        assertEquals("i-initial", linkSwappedTo)

        // Registrations restored back to initialManifest
        assertEquals(initialManifest, File(pluginsDir, "fastboot.json").readText())

        val fold = journal.fold()
        assertEquals(ToolchainState.READY, fold.state)
        assertEquals("i-initial", fold.activeInstallId)
    }

    @Test
    fun `restore failure results in MAINTENANCE_FAILED and RECOVERY_REQUIRED`() {
        createInstall("i-from-01", "fastboot", "fastboot")
        createInstall("i-to-0001", "fastboot", "fastboot")
        val journal = ToolchainJournal(stateDir)

        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal,
            validator = createValidator(),
            activeInstallProvider = { "i-from-01" },
            verifyProvider = { false }, // candidate verification fails
            restoreVerifier = { false }, // restore re-verification also fails!
            linkSwapper = ::mockLinkSwapper
        )

        val outcome = activator.activate(
            requestId = "req-maint-fail",
            expectedActive = "i-from-01",
            targetInstallId = "i-to-0001"
        )

        assertIs<ActivationOutcome.MaintenanceFailed>(outcome)

        val fold = journal.fold()
        assertEquals(ToolchainState.RECOVERY_REQUIRED, fold.state)
        assertEquals("req-maint-fail", fold.recoveryRequiredBy)
    }

    @Test
    fun `a run started between the check and the hold gets blocked`() {
        createInstall("i-target", "adb", "adb")
        val journal = ToolchainJournal(stateDir)
        
        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal,
            validator = createValidator(),
            tryAddHold = { listOf("run-started-concurrently") },
            linkSwapper = ::mockLinkSwapper
        )

        val outcome = activator.activate("req-concurrent", null, "i-target")
        val blocked = assertIs<ActivationOutcome.Blocked>(outcome)
        assertEquals(listOf("run-started-concurrently"), blocked.activeWork)
    }

    @Test
    fun `a fake registry that doesn't resolve a tool fails verification and restores the previous install`() {
        createInstall("i-initial", "fastboot", "fastboot")
        createInstall("i-target", "missing_tool", "missing_tool")
        val journal = ToolchainJournal(stateDir)

        val fakeRegistry = object : io.ltirom.server.domain.ports.ToolResolverPort {
            override fun resolve(toolName: String): File? = null
            override fun listAvailableTools(): Map<String, Boolean> = emptyMap()
            override fun listToolStatuses() = emptyList<io.ltirom.server.model.ToolStatusInfo>()
            override fun resolveSource(toolName: String) = null
        }

        val activator = ToolchainActivator(
            toolsDir = toolsDir,
            stateDir = stateDir,
            journal = journal,
            validator = createValidator(),
            activeInstallProvider = { "i-initial" },
            linkSwapper = ::mockLinkSwapper,
            verifyProvider = { targetInstallId ->
                val targetJson = File(toolsDir, "installs/$targetInstallId/install.json")
                val manifest = kotlinx.serialization.json.Json { ignoreUnknownKeys = true }.decodeFromString<InstallManifest>(targetJson.readText())
                manifest.outputs.all { output ->
                    fakeRegistry.resolve(output.toolId) != null
                }
            }
        )

        val outcome = activator.activate("req-fake-reg", "i-initial", "i-target")
        assertIs<ActivationOutcome.VerifyFailedRestored>(outcome)
        assertEquals("i-initial", outcome.activeInstallId)
    }
}
