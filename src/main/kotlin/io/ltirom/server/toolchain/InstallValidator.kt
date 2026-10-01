package io.ltirom.server.toolchain

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.File
import java.security.MessageDigest

public sealed interface ValidationResult {
    public data object Valid : ValidationResult
    public data class Invalid(val reason: String) : ValidationResult
}

@Serializable
public data class InstallManifestOutput(
    val toolId: String,
    val file: String,
    val kind: String? = null,
    val requiredForProduct: Boolean = true
)

@Serializable
public data class InstallManifest(
    val schema: Int = 1,
    val installId: String? = null,
    val distro: String,
    val arch: String,
    val groups: Map<String, String> = emptyMap(),
    val outputs: List<InstallManifestOutput> = emptyList(),
    val catalogRevision: Int = 1,
    val createdAt: Long = 0L
)

@Serializable
public data class ArtifactFileEntry(
    val path: String,
    val sha256: String,
    val mode: String? = null,
    val linkTarget: String? = null
)

@Serializable
internal data class ArtifactFileManifest(
    val schema: Int = 1,
    val group: String? = null,
    val artifactId: String? = null,
    val files: List<ArtifactFileEntry> = emptyList(),
    val outputs: List<String> = emptyList()
)

public class InstallValidator(
    private val toolsDir: File,
    private val serviceDistro: String,
    private val serviceArch: String,
    private val realPathResolver: (File) -> File? = { file ->
        runCatching { file.toPath().toRealPath().toFile() }.getOrNull()
    }
) {
    private val json = Json { ignoreUnknownKeys = true }
    private val installIdRegex = Regex("^[a-z0-9-]{8,64}$")

    public fun isValidInstallId(id: String): Boolean {
        if (id.contains("..") || id.contains("/") || id.contains("\\")) return false
        return installIdRegex.matches(id)
    }

    public fun validate(targetInstallId: String): ValidationResult {
        if (!isValidInstallId(targetInstallId)) {
            return ValidationResult.Invalid("Invalid install ID format: '$targetInstallId'")
        }

        val installsBase = File(toolsDir, "installs")
        val installDir = File(installsBase, targetInstallId)
        if (!installDir.exists() || !installDir.isDirectory) {
            return ValidationResult.Invalid("Install directory does not exist: ${installDir.path}")
        }

        val installsReal = runCatching { installsBase.toPath().toRealPath() }.getOrNull()
        val installReal = realPathResolver(installDir)?.toPath()
        if (installsReal == null || installReal == null || !installReal.startsWith(installsReal)) {
            return ValidationResult.Invalid("Install directory resolves outside installs directory")
        }

        val installJsonFile = File(installDir, "install.json")
        if (!installJsonFile.exists()) {
            return ValidationResult.Invalid("install.json missing in ${installDir.path}")
        }

        val manifest = try {
            json.decodeFromString<InstallManifest>(installJsonFile.readText())
        } catch (e: Exception) {
            return ValidationResult.Invalid("Invalid install.json content: ${e.message}")
        }

        if (manifest.schema != 1) {
            return ValidationResult.Invalid("Unsupported schema version: ${manifest.schema}")
        }

        if (manifest.distro != serviceDistro || manifest.arch != serviceArch) {
            return ValidationResult.Invalid(
                "distro or arch mismatch: expected $serviceDistro/$serviceArch, got ${manifest.distro}/${manifest.arch}"
            )
        }

        val binDir = File(installDir, "bin")
        if (!binDir.exists() || !binDir.isDirectory) {
            return ValidationResult.Invalid("bin directory missing in ${installDir.path}")
        }

        val artifactsBase = File(toolsDir, "artifacts")
        val artifactsReal = runCatching { artifactsBase.toPath().toRealPath() }.getOrNull()

        for (output in manifest.outputs) {
            val binFile = File(binDir, output.file)
            if (!binFile.exists()) {
                return ValidationResult.Invalid("missing output: ${output.file} in bin directory")
            }

            val realBin = realPathResolver(binFile)?.toPath()
            if (realBin == null || artifactsReal == null || !realBin.startsWith(artifactsReal)) {
                return ValidationResult.Invalid("Output symlink resolves outside artifacts directory: ${output.file}")
            }

            var artDir: File? = null
            var curr: File? = realBin.toFile().parentFile
            var candidateGroupOrArt: File? = null
            while (curr != null && curr != artifactsBase && curr.toPath().toRealPath() != artifactsReal) {
                if (File(curr, "manifest.json").exists() || File(curr, "unusable.json").exists()) {
                    artDir = curr
                    break
                }
                if (curr.parentFile?.parentFile == artifactsBase || curr.parentFile == artifactsBase) {
                    candidateGroupOrArt = curr
                }
                curr = curr.parentFile
            }
            if (artDir == null) {
                artDir = candidateGroupOrArt ?: realBin.toFile().parentFile
            }

            if (File(artDir, "unusable.json").exists()) {
                return ValidationResult.Invalid("Artifact is marked unusable: ${artDir.name}")
            }

            val manifestFile = File(artDir, "manifest.json")
            if (manifestFile.exists()) {
                val rootElem = try {
                    json.parseToJsonElement(manifestFile.readText())
                } catch (e: Exception) {
                    return ValidationResult.Invalid("Invalid manifest.json in ${artDir.path}: ${e.message}")
                }
                val filesElem = (rootElem as? kotlinx.serialization.json.JsonObject)?.get("files")
                val expectedSha: String? = when (filesElem) {
                    is kotlinx.serialization.json.JsonArray -> {
                        filesElem.firstOrNull { elem ->
                            val path = (elem as? kotlinx.serialization.json.JsonObject)?.get("path")?.let {
                                (it as? kotlinx.serialization.json.JsonPrimitive)?.content
                            } ?: ""
                            path == "bin/${output.file}" || path == output.file || path.endsWith("/${output.file}")
                        }?.let {
                            (it as? kotlinx.serialization.json.JsonObject)?.get("sha256")?.let { s ->
                                (s as? kotlinx.serialization.json.JsonPrimitive)?.content
                            }
                        }
                    }
                    is kotlinx.serialization.json.JsonObject -> {
                        (filesElem[output.file] as? kotlinx.serialization.json.JsonPrimitive)?.content
                            ?: (filesElem["bin/${output.file}"] as? kotlinx.serialization.json.JsonPrimitive)?.content
                    }
                    else -> null
                }
                if (expectedSha == null) {
                    return ValidationResult.Invalid("Missing checksum evidence for ${output.file} in ${artDir.name}")
                }
                val actualSha = sha256(realBin.toFile().readBytes())
                if (!actualSha.equals(expectedSha, ignoreCase = true)) {
                    return ValidationResult.Invalid(
                        "sha256 mismatch for ${output.file}: expected $expectedSha, got $actualSha"
                    )
                }
            }
        }

        return ValidationResult.Valid
    }

    private fun sha256(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
}
