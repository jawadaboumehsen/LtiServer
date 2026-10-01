package io.ltirom.server.toolchain

import java.io.File
import java.nio.file.Files
import java.security.MessageDigest
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class InstallValidatorTest {

    private lateinit var tempDir: File
    private val simulatedSymlinks = mutableMapOf<File, File>()

    @BeforeTest
    fun setUp() {
        tempDir = Files.createTempDirectory("validator-test").toFile()
        simulatedSymlinks.clear()
    }

    @AfterTest
    fun tearDown() {
        tempDir.deleteRecursively()
    }

    private fun sha256(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }

    private fun setupEnvironment(
        distro: String = "Ubuntu-22.04",
        arch: String = "x86_64"
    ): Pair<File, InstallValidator> {
        val toolsDir = File(tempDir, "LtiRomTools").apply { mkdirs() }
        File(toolsDir, "installs").mkdirs()
        File(toolsDir, "artifacts").mkdirs()
        val validator = InstallValidator(
            toolsDir = toolsDir,
            serviceDistro = distro,
            serviceArch = arch,
            realPathResolver = { file ->
                val simulated = simulatedSymlinks[file]
                if (simulated != null) {
                    runCatching { simulated.toPath().toRealPath().toFile() }.getOrDefault(simulated)
                } else {
                    runCatching { file.toPath().toRealPath().toFile() }.getOrNull()
                }
            }
        )
        return toolsDir to validator
    }

    private fun createSymlink(link: File, target: File) {
        val created = runCatching {
            Files.createSymbolicLink(link.toPath(), target.toPath())
        }.isSuccess
        if (!created) {
            link.writeText("simulated-symlink")
            simulatedSymlinks[link] = target
        }
    }

    @Test
    fun `validates valid install ID pattern`() {
        val (_, validator) = setupEnvironment()
        assertTrue(!validator.isValidInstallId("legacy"))
        assertTrue(validator.isValidInstallId("i-12345678abcdef01"))
        assertTrue(validator.isValidInstallId("valid-install-id-12345678"))

        // Invalid IDs
        assertTrue(!validator.isValidInstallId("../escaped"))
        assertTrue(!validator.isValidInstallId("foo/bar"))
        assertTrue(!validator.isValidInstallId("short")) // < 8 chars
        assertTrue(!validator.isValidInstallId("INVALID_UPPERCASE_12345678"))
        assertTrue(!validator.isValidInstallId("has spaces 12345678"))
    }

    @Test
    fun `rejects invalid install ID format`() {
        val (_, validator) = setupEnvironment()
        val res = validator.validate("..")
        val inv = assertIs<ValidationResult.Invalid>(res)
        assertTrue(inv.reason.contains("invalid install ID format", ignoreCase = true))
    }

    @Test
    fun `rejects path escaping installs root`() {
        val (toolsDir, validator) = setupEnvironment()
        val outsideDir = File(tempDir, "outside-install").apply { mkdirs() }
        // Symlink inside installs pointing outside
        val symlinkInside = File(toolsDir, "installs/i-symlink-escape")
        runCatching {
            Files.createSymbolicLink(symlinkInside.toPath(), outsideDir.toPath())
        }

        if (symlinkInside.exists()) {
            val res = validator.validate("i-symlink-escape")
            val inv = assertIs<ValidationResult.Invalid>(res)
            assertTrue(inv.reason.contains("outside installs directory", ignoreCase = true))
        }
    }

    @Test
    fun `rejects non-existent install directory`() {
        val (_, validator) = setupEnvironment()
        val res = validator.validate("i-nonexistent-12345")
        val inv = assertIs<ValidationResult.Invalid>(res)
        assertTrue(inv.reason.contains("does not exist", ignoreCase = true))
    }

    @Test
    fun `rejects missing install json`() {
        val (toolsDir, validator) = setupEnvironment()
        val installDir = File(toolsDir, "installs/i-missing-json-12345").apply { mkdirs() }
        File(installDir, "bin").mkdirs()

        val res = validator.validate("i-missing-json-12345")
        val inv = assertIs<ValidationResult.Invalid>(res)
        assertTrue(inv.reason.contains("install.json missing", ignoreCase = true))
    }

    @Test
    fun `rejects unsupported schema version`() {
        val (toolsDir, validator) = setupEnvironment()
        val installDir = File(toolsDir, "installs/i-bad-schema-12345").apply { mkdirs() }
        File(installDir, "install.json").writeText(
            """
            {
              "schema": 999,
              "distro": "Ubuntu-22.04",
              "arch": "x86_64",
              "outputs": []
            }
            """.trimIndent()
        )
        val res = validator.validate("i-bad-schema-12345")
        val inv = assertIs<ValidationResult.Invalid>(res)
        assertTrue(inv.reason.contains("schema", ignoreCase = true))
    }

    @Test
    fun `rejects distro or arch mismatch`() {
        val (toolsDir, validator) = setupEnvironment(distro = "Ubuntu-22.04", arch = "x86_64")
        val installDir = File(toolsDir, "installs/i-distro-mismatch-123").apply { mkdirs() }
        File(installDir, "install.json").writeText(
            """
            {
              "schema": 1,
              "distro": "Debian-12",
              "arch": "x86_64",
              "outputs": []
            }
            """.trimIndent()
        )
        val res = validator.validate("i-distro-mismatch-123")
        val inv = assertIs<ValidationResult.Invalid>(res)
        assertTrue(inv.reason.contains("distro or arch mismatch", ignoreCase = true))
    }

    @Test
    fun `rejects missing listed output in bin`() {
        val (toolsDir, validator) = setupEnvironment()
        val installDir = File(toolsDir, "installs/i-missing-output-123").apply { mkdirs() }
        File(installDir, "bin").mkdirs()
        File(installDir, "install.json").writeText(
            """
            {
              "schema": 1,
              "distro": "Ubuntu-22.04",
              "arch": "x86_64",
              "outputs": [
                { "toolId": "fastboot", "file": "fastboot" }
              ]
            }
            """.trimIndent()
        )
        val res = validator.validate("i-missing-output-123")
        val inv = assertIs<ValidationResult.Invalid>(res)
        assertTrue(inv.reason.contains("missing output: fastboot", ignoreCase = true))
    }

    @Test
    fun `rejects symlink in bin resolving outside artifacts directory`() {
        val (toolsDir, validator) = setupEnvironment()
        val installDir = File(toolsDir, "installs/i-symlink-escape-bin").apply { mkdirs() }
        val binDir = File(installDir, "bin").apply { mkdirs() }
        val outsideTarget = File(tempDir, "evil-binary").apply { writeText("bin") }

        val symlinkFile = File(binDir, "fastboot")
        createSymlink(symlinkFile, outsideTarget)

        File(installDir, "install.json").writeText(
            """
            {
              "schema": 1,
              "distro": "Ubuntu-22.04",
              "arch": "x86_64",
              "outputs": [
                { "toolId": "fastboot", "file": "fastboot" }
              ]
            }
            """.trimIndent()
        )

        val res = validator.validate("i-symlink-escape-bin")
        val inv = assertIs<ValidationResult.Invalid>(res)
        assertTrue(inv.reason.contains("resolves outside artifacts", ignoreCase = true))
    }

    @Test
    fun `rejects artifact marked with unusable json`() {
        val (toolsDir, validator) = setupEnvironment()
        val artifactDir = File(toolsDir, "artifacts/art-12345678-abcd").apply { mkdirs() }
        val binInArt = File(artifactDir, "fastboot").apply { writeText("fastboot binary") }
        File(artifactDir, "unusable.json").writeText("""{"reason":"broken"}""")

        val installDir = File(toolsDir, "installs/i-unusable-art-12345").apply { mkdirs() }
        val binDir = File(installDir, "bin").apply { mkdirs() }
        val symlinkFile = File(binDir, "fastboot")
        createSymlink(symlinkFile, binInArt)

        File(installDir, "install.json").writeText(
            """
            {
              "schema": 1,
              "distro": "Ubuntu-22.04",
              "arch": "x86_64",
              "outputs": [
                { "toolId": "fastboot", "file": "fastboot" }
              ]
            }
            """.trimIndent()
        )

        val res = validator.validate("i-unusable-art-12345")
        val inv = assertIs<ValidationResult.Invalid>(res)
        assertTrue(inv.reason.contains("marked unusable", ignoreCase = true))
    }

    @Test
    fun `rejects sha256 mismatch against manifest`() {
        val (toolsDir, validator) = setupEnvironment()
        val artifactDir = File(toolsDir, "artifacts/art-sha-mismatch-123").apply { mkdirs() }
        val binInArt = File(artifactDir, "fastboot").apply { writeText("tampered binary content") }
        File(artifactDir, "manifest.json").writeText(
            """
            {
              "files": {
                "fastboot": "0000000000000000000000000000000000000000000000000000000000000000"
              }
            }
            """.trimIndent()
        )

        val installDir = File(toolsDir, "installs/i-sha-mismatch-12345").apply { mkdirs() }
        val binDir = File(installDir, "bin").apply { mkdirs() }
        val symlinkFile = File(binDir, "fastboot")
        createSymlink(symlinkFile, binInArt)

        File(installDir, "install.json").writeText(
            """
            {
              "schema": 1,
              "distro": "Ubuntu-22.04",
              "arch": "x86_64",
              "outputs": [
                { "toolId": "fastboot", "file": "fastboot" }
              ]
            }
            """.trimIndent()
        )

        val res = validator.validate("i-sha-mismatch-12345")
        val inv = assertIs<ValidationResult.Invalid>(res)
        assertTrue(inv.reason.contains("sha256 mismatch", ignoreCase = true))
    }

    @Test
    fun `accepts valid install with matching artifacts and manifest`() {
        val (toolsDir, validator) = setupEnvironment()
        val artifactDir = File(toolsDir, "artifacts/art-valid-12345678").apply { mkdirs() }
        val binaryContent = "valid binary fastboot content".toByteArray()
        val expectedSha = sha256(binaryContent)
        val binInArt = File(artifactDir, "fastboot").apply { writeBytes(binaryContent) }
        File(artifactDir, "manifest.json").writeText(
            """
            {
              "files": {
                "fastboot": "$expectedSha"
              }
            }
            """.trimIndent()
        )

        val installDir = File(toolsDir, "installs/i-valid-install-12345").apply { mkdirs() }
        val binDir = File(installDir, "bin").apply { mkdirs() }
        val symlinkFile = File(binDir, "fastboot")
        createSymlink(symlinkFile, binInArt)

        File(installDir, "install.json").writeText(
            """
            {
              "schema": 1,
              "distro": "Ubuntu-22.04",
              "arch": "x86_64",
              "outputs": [
                { "toolId": "fastboot", "file": "fastboot" }
              ]
            }
            """.trimIndent()
        )

        val res = validator.validate("i-valid-install-12345")
        assertEquals(ValidationResult.Valid, res)
    }
}
