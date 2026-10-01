package io.ltirom.server.toolchain

import io.ltirom.server.engine.Hold
import io.ltirom.server.infra.ToolManifest
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.File
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.security.MessageDigest

public sealed interface ActivationOutcome {
    public data class Committed(val activeInstallId: String) : ActivationOutcome
    public data class Blocked(val activeWork: List<String>) : ActivationOutcome
    public data class Rejected(
        val code: String,
        val reason: String? = null,
        val actualActiveInstallId: String? = null,
        val conflictingToolIds: List<String> = emptyList()
    ) : ActivationOutcome
    public data class VerifyFailedRestored(val reason: String, val activeInstallId: String?) : ActivationOutcome
    public data class MaintenanceFailed(val reason: String) : ActivationOutcome
    public data object InProgress : ActivationOutcome
}

@Serializable
internal data class RejectionPayload(
    val code: String,
    val actual: String? = null,
    val reason: String? = null,
    val activeWork: List<String> = emptyList(),
    val toolIds: List<String> = emptyList(),
    val requestHash: String? = null
)

@Serializable
internal data class RequestPayload(
    val from: String?,
    val to: String?,
    val requestHash: String
)

@Serializable
internal data class LinkPayload(
    val target: String?
)

@Serializable
internal data class RegsAppliedPayload(
    val toolIds: List<String>
)

@Serializable
internal data class CommitPayload(
    val activeInstallId: String?
)

@Serializable
internal data class RestoreStartedPayload(
    val target: String?,
    val reason: String
)

@Serializable
internal data class RestoredPayload(
    val activeInstallId: String?
)

@Serializable
internal data class MaintenanceFailedPayload(
    val reason: String
)

public open class ToolchainActivator(
    private val toolsDir: File,
    private val stateDir: File,
    private val journal: ToolchainJournal,
    private val validator: InstallValidator = InstallValidator(toolsDir, "Ubuntu-Test", "x86_64"),
    private val activeInstallProvider: () -> String? = { readActiveInstall(toolsDir) },
    private val tryAddHold: (Hold) -> List<String> = { emptyList() },
    // holdAdder removed
    private val holdRemover: (Hold) -> Unit = {},
    private val verifyProvider: (targetInstallId: String) -> Boolean = { true },
    private val restoreVerifier: (fromInstallId: String?) -> Boolean = { true },
    private val linkSwapper: (targetInstallId: String?) -> Unit = { target -> swapLink(toolsDir, target) }
) {
    private val pluginsDir = File(stateDir, "tools.d")
    private val json = Json { ignoreUnknownKeys = true; prettyPrint = false }
    private val mutex = Any()

    init {
        pluginsDir.mkdirs()
    }

    public companion object {
        public const val TOOLCHAIN_OWNER: String = "ltirom-toolchain"

        public fun computeRequestHash(expectedActive: String?, targetInstallId: String?): String {
            val exp = expectedActive ?: "-"
            val tgt = targetInstallId ?: "-"
            return sha256("v1\n$exp\n$tgt")
        }

        public fun sha256(text: String): String =
            MessageDigest.getInstance("SHA-256").digest(text.toByteArray(Charsets.UTF_8))
                .joinToString("") { "%02x".format(it) }

        public fun swapLink(toolsDir: File, targetInstallId: String?) {
            val binDir = File(toolsDir, "bin")
            val binPath = binDir.toPath()
            if (targetInstallId == null) {
                if (Files.isSymbolicLink(binPath)) {
                    Files.deleteIfExists(binPath)
                }
                return
            }
            val targetBin = File(toolsDir, "installs/$targetInstallId/bin")
            val targetBinPath = targetBin.toPath()

            val tmpLink = File(toolsDir, "bin.tmp-${System.nanoTime()}")
            Files.createSymbolicLink(tmpLink.toPath(), targetBinPath)

            try {
                Files.move(
                    tmpLink.toPath(),
                    binPath,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
                )
            } catch (_: Exception) {
                if (Files.isSymbolicLink(binPath)) {
                    Files.deleteIfExists(binPath)
                }
                Files.move(
                    tmpLink.toPath(),
                    binPath,
                    StandardCopyOption.REPLACE_EXISTING
                )
            }
        }

        public fun readActiveInstall(toolsDir: File): String? {
            val binDir = File(toolsDir, "bin")
            if (!binDir.exists()) return null
            val real = runCatching { binDir.toPath().toRealPath() }.getOrNull() ?: return null
            return real.parent?.fileName?.toString()
        }

        public fun verifyInstall(
            toolsDir: File,
            targetInstallId: String,
            registry: io.ltirom.server.infra.DynamicToolRegistry? = null,
            runner: io.ltirom.server.domain.ports.ProcessExecutionPort? = null
        ): Boolean {
            val installJson = File(toolsDir, "installs/$targetInstallId/install.json")
            if (!installJson.exists()) return false
            val manifest = runCatching {
                Json { ignoreUnknownKeys = true }.decodeFromString<InstallManifest>(installJson.readText())
            }.getOrNull() ?: return false
            val binDir = File(toolsDir, "installs/$targetInstallId/bin")
            if (!binDir.exists() || !binDir.isDirectory) return false
            for (output in manifest.outputs) {
                val f = File(binDir, output.file)
                if (!f.exists()) return false
                if (!f.canExecute() && !f.canRead()) return false
            }

            registry?.refresh()

            if (registry != null) {
                val binNormalized = File(toolsDir, "bin").absolutePath.replace("\\", "/")
                val toolsDirCanonical = toolsDir.canonicalPath.replace("\\", "/")
                for (output in manifest.outputs) {
                    val status = registry.getToolStatus(output.toolId)
                    if (status == null || !status.installed || status.source != "DYNAMIC") {
                        return false
                    }
                    val statusPathNormalized = File(status.path).absolutePath.replace("\\", "/")
                    if (!statusPathNormalized.startsWith(binNormalized)) {
                        return false
                    }
                    val pathCanonical = File(status.path).canonicalPath.replace("\\", "/")
                    if (!pathCanonical.startsWith(toolsDirCanonical)) {
                        return false
                    }
                    val targetFile = File(pathCanonical)
                    if (!targetFile.exists() || (!targetFile.canExecute() && !targetFile.canRead())) {
                        return false
                    }
                }
            }

            if (runner != null) {
                for (output in manifest.outputs) {
                    val probeArgs = when (output.toolId) {
                        "fastboot" -> listOf("--version")
                        "adb" -> listOf("version")
                        "mke2fs" -> listOf("-V")
                        else -> listOf("--help")
                    }
                    val probeResult = kotlinx.coroutines.runBlocking {
                        runCatching {
                            runner.runProcess(
                                toolName = output.toolId,
                                arguments = probeArgs,
                                workingDir = toolsDir,
                                environment = emptyMap(),
                                stdinText = null,
                                timeoutMs = 5000L,
                                onOutput = null
                            )
                        }.getOrNull()
                    }
                    if (probeResult == null || probeResult.exitCode < 0) {
                        return false
                    }
                }
            }

            return true
        }
    }

    public open fun activate(
        requestId: String,
        expectedActive: String?,
        targetInstallId: String
    ): ActivationOutcome = synchronized(mutex) {
        val requestHash = computeRequestHash(expectedActive, targetInstallId)
        val foldBefore = journal.fold()
        val existing = foldBefore.activations[requestId]

        // 1. Replay check
        if (existing != null) {
            if (existing.requestHash != requestHash) {
                return ActivationOutcome.Rejected("REQUEST_MISMATCH", "Request hash mismatch")
            }
            return when (existing.state) {
                ActivationState.SWITCHING -> ActivationOutcome.InProgress
                ActivationState.COMMITTED -> ActivationOutcome.Committed(existing.activeInstallId ?: targetInstallId)
                ActivationState.RESTORED -> ActivationOutcome.VerifyFailedRestored("Previously restored", existing.activeInstallId)
                ActivationState.MAINTENANCE_FAILED -> ActivationOutcome.MaintenanceFailed("Previously failed maintenance")
                ActivationState.REJECTED -> ActivationOutcome.Rejected(existing.rejectionCode ?: "REJECTED")
            }
        }

        // 2. Stale check
        val actualActive = activeInstallProvider()
        if (actualActive != expectedActive) {
            journal.append(
                requestId,
                JournalEntryType.REJECTED,
                json.encodeToString(
                    RejectionPayload(
                        code = "CONFLICT",
                        actual = actualActive,
                        requestHash = requestHash
                    )
                )
            )
            return ActivationOutcome.Rejected("CONFLICT", actualActiveInstallId = actualActive)
        }

        // 3. Under admission check
        val hold = Hold.Activation(requestId)
        val activeWork = tryAddHold(hold)
        if (activeWork.isNotEmpty()) {
            journal.append(
                requestId,
                JournalEntryType.REJECTED,
                json.encodeToString(
                    RejectionPayload(
                        code = "BLOCKED",
                        activeWork = activeWork,
                        requestHash = requestHash
                    )
                )
            )
            return ActivationOutcome.Blocked(activeWork)
        }

        try {
            // 4. Validate target
            val validation = validator.validate(targetInstallId)
            if (validation is ValidationResult.Invalid) {
                journal.append(
                    requestId,
                    JournalEntryType.REJECTED,
                    json.encodeToString(
                        RejectionPayload(
                            code = "INVALID_TARGET",
                            reason = validation.reason,
                            requestHash = requestHash
                        )
                    )
                )
                return ActivationOutcome.Rejected("INVALID_TARGET", reason = validation.reason)
            }

            // 5. Journal REQUEST
            journal.append(
                requestId,
                JournalEntryType.REQUEST,
                json.encodeToString(
                    RequestPayload(
                        from = expectedActive,
                        to = targetInstallId,
                        requestHash = requestHash
                    )
                )
            )

            // 6. Stage registrations
            val targetInstallJson = File(toolsDir, "installs/$targetInstallId/install.json")
            val manifest = json.decodeFromString<InstallManifest>(targetInstallJson.readText())
            val conflicts = checkRegistrationConflicts(manifest.outputs)
            if (conflicts.isNotEmpty()) {
                journal.append(
                    requestId,
                    JournalEntryType.REJECTED,
                    json.encodeToString(
                        RejectionPayload(
                            code = "REGISTRATION_CONFLICT",
                            toolIds = conflicts,
                            requestHash = requestHash
                        )
                    )
                )
                return ActivationOutcome.Rejected("REGISTRATION_CONFLICT", conflictingToolIds = conflicts)
            }

            val snapshot = computeRegistrationSnapshot(manifest.outputs)
            journal.append(
                requestId,
                JournalEntryType.REG_SNAPSHOT,
                json.encodeToString(snapshot)
            )

            // 7. Swap bin link and apply staged registrations
            journal.append(
                requestId,
                JournalEntryType.LINK_INTENT,
                json.encodeToString(LinkPayload(targetInstallId))
            )
            linkSwapper(targetInstallId)
            journal.append(
                requestId,
                JournalEntryType.LINK_SWAPPED,
                json.encodeToString(LinkPayload(targetInstallId))
            )

            applyOwnedRegistrations(manifest.outputs, File(toolsDir, "bin"))
            journal.append(
                requestId,
                JournalEntryType.REGS_APPLIED,
                json.encodeToString(RegsAppliedPayload(manifest.outputs.map { it.toolId }))
            )

            // 8. Internal verification
            val verifyPassed = verifyProvider(targetInstallId)

            // 9. Success -> Commit
            if (verifyPassed) {
                journal.append(
                    requestId,
                    JournalEntryType.VERIFIED,
                    json.encodeToString(CommitPayload(targetInstallId))
                )
                journal.append(
                    requestId,
                    JournalEntryType.COMMITTED,
                    json.encodeToString(CommitPayload(targetInstallId))
                )
                return ActivationOutcome.Committed(targetInstallId)
            }

            // 10. Failure -> Restore
            journal.append(
                requestId,
                JournalEntryType.RESTORE_STARTED,
                json.encodeToString(RestoreStartedPayload(expectedActive, "Verification failed"))
            )
            linkSwapper(expectedActive)
            journal.append(
                requestId,
                JournalEntryType.RESTORE_LINKED,
                json.encodeToString(LinkPayload(expectedActive))
            )

            restoreRegistrationSnapshot(snapshot)

            val restoreOk = if (expectedActive != null) restoreVerifier(expectedActive) else true
            if (restoreOk) {
                journal.append(
                    requestId,
                    JournalEntryType.RESTORED,
                    json.encodeToString(RestoredPayload(expectedActive))
                )
                return ActivationOutcome.VerifyFailedRestored("Verification failed", expectedActive)
            } else {
                journal.append(
                    requestId,
                    JournalEntryType.MAINTENANCE_FAILED,
                    json.encodeToString(MaintenanceFailedPayload("Restore verification failed"))
                )
                return ActivationOutcome.MaintenanceFailed("Restore verification failed")
            }
        } finally {
            holdRemover(hold)
        }
    }

    public fun recoverStartup(): Unit = synchronized(mutex) {
        val fold = journal.fold()
        val inFlight = fold.inFlightTransaction
        val recoveryReq = fold.recoveryRequiredBy

        if (inFlight != null) {
            val observedBin = activeInstallProvider()
            when (inFlight.lastIntentType) {
                JournalEntryType.REQUEST -> {
                    if (observedBin == inFlight.from) {
                        journal.append(
                            inFlight.requestId,
                            JournalEntryType.RESTORED,
                            json.encodeToString(RestoredPayload(inFlight.from))
                        )
                    } else {
                        journal.append(
                            inFlight.requestId,
                            JournalEntryType.MAINTENANCE_FAILED,
                            json.encodeToString(
                                MaintenanceFailedPayload(
                                    "Observed bin mismatch during REQUEST recovery: observed=$observedBin, expected=${inFlight.from}"
                                )
                            )
                        )
                    }
                }
                JournalEntryType.REG_SNAPSHOT -> {
                    if (observedBin == inFlight.from) {
                        restoreRegistrationSnapshot(inFlight.regSnapshot)
                        journal.append(
                            inFlight.requestId,
                            JournalEntryType.RESTORED,
                            json.encodeToString(RestoredPayload(inFlight.from))
                        )
                    } else {
                        journal.append(
                            inFlight.requestId,
                            JournalEntryType.MAINTENANCE_FAILED,
                            json.encodeToString(MaintenanceFailedPayload("Unreadable bin target during REG_SNAPSHOT recovery"))
                        )
                    }
                }
                JournalEntryType.LINK_INTENT -> {
                    if (observedBin == inFlight.to) {
                        val targetJson = File(toolsDir, "installs/${inFlight.to}/install.json")
                        if (targetJson.exists()) {
                            val manifest = json.decodeFromString<InstallManifest>(targetJson.readText())
                            applyOwnedRegistrations(manifest.outputs, File(toolsDir, "bin"))
                        }
                        if (verifyProvider(inFlight.to ?: "")) {
                            journal.append(
                                inFlight.requestId,
                                JournalEntryType.VERIFIED,
                                json.encodeToString(CommitPayload(inFlight.to))
                            )
                            journal.append(
                                inFlight.requestId,
                                JournalEntryType.COMMITTED,
                                json.encodeToString(CommitPayload(inFlight.to))
                            )
                        } else {
                            rollbackInFlight(inFlight)
                        }
                    } else if (observedBin == inFlight.from) {
                        restoreRegistrationSnapshot(inFlight.regSnapshot)
                        journal.append(
                            inFlight.requestId,
                            JournalEntryType.RESTORED,
                            json.encodeToString(RestoredPayload(inFlight.from))
                        )
                    } else {
                        journal.append(
                            inFlight.requestId,
                            JournalEntryType.MAINTENANCE_FAILED,
                            json.encodeToString(MaintenanceFailedPayload("Observed bin foreign"))
                        )
                    }
                }
                JournalEntryType.RESTORE_STARTED -> {
                    if (observedBin == inFlight.to) {
                        linkSwapper(inFlight.from)
                        journal.append(
                            inFlight.requestId,
                            JournalEntryType.RESTORE_LINKED,
                            json.encodeToString(LinkPayload(inFlight.from))
                        )
                    }
                    restoreRegistrationSnapshot(inFlight.regSnapshot)
                    val ok = if (inFlight.from != null) restoreVerifier(inFlight.from) else true
                    if (ok) {
                        journal.append(
                            inFlight.requestId,
                            JournalEntryType.RESTORED,
                            json.encodeToString(RestoredPayload(inFlight.from))
                        )
                    } else {
                        journal.append(
                            inFlight.requestId,
                            JournalEntryType.MAINTENANCE_FAILED,
                            json.encodeToString(MaintenanceFailedPayload("Restore verification failed during recovery"))
                        )
                    }
                }
                else -> {
                    if (observedBin != inFlight.from && observedBin != inFlight.to) {
                        journal.append(
                            inFlight.requestId,
                            JournalEntryType.MAINTENANCE_FAILED,
                            json.encodeToString(MaintenanceFailedPayload("Observed bin is neither from nor to"))
                        )
                    }
                }
            }
        } else if (recoveryReq != null) {
            val record = fold.activations[recoveryReq]
            if (record != null) {
                linkSwapper(record.from)
                if (record.from != null) {
                    val fromJson = File(toolsDir, "installs/${record.from}/install.json")
                    if (fromJson.exists()) {
                        val manifest = runCatching { json.decodeFromString<InstallManifest>(fromJson.readText()) }.getOrNull()
                        if (manifest != null) {
                            applyOwnedRegistrations(manifest.outputs, File(toolsDir, "bin"))
                        }
                    }
                } else {
                    applyOwnedRegistrations(emptyList(), File(toolsDir, "bin"))
                }
                val ok = if (record.from != null) restoreVerifier(record.from) else true
                if (ok) {
                    journal.append(
                        record.requestId,
                        JournalEntryType.RESTORED,
                        json.encodeToString(RestoredPayload(record.from))
                    )
                }
            }
        }
    }

    private fun rollbackInFlight(inFlight: InFlightTransaction) {
        journal.append(
            inFlight.requestId,
            JournalEntryType.RESTORE_STARTED,
            json.encodeToString(RestoreStartedPayload(inFlight.from, "Recovery rollback"))
        )
        linkSwapper(inFlight.from)
        journal.append(
            inFlight.requestId,
            JournalEntryType.RESTORE_LINKED,
            json.encodeToString(LinkPayload(inFlight.from))
        )
        restoreRegistrationSnapshot(inFlight.regSnapshot)
        val ok = if (inFlight.from != null) restoreVerifier(inFlight.from) else true
        if (ok) {
            journal.append(
                inFlight.requestId,
                JournalEntryType.RESTORED,
                json.encodeToString(RestoredPayload(inFlight.from))
            )
        } else {
            journal.append(
                inFlight.requestId,
                JournalEntryType.MAINTENANCE_FAILED,
                json.encodeToString(MaintenanceFailedPayload("Rollback verification failed during recovery"))
            )
        }
    }

    public fun checkRegistrationConflicts(targetOutputs: List<InstallManifestOutput>): List<String> {
        val conflicts = mutableListOf<String>()
        val files = pluginsDir.listFiles { _, name -> name.endsWith(".json") }.orEmpty()
        val targetToolIds = targetOutputs.map { it.toolId }.toSet()

        for (f in files) {
            val manifest = runCatching { json.decodeFromString<ToolManifest>(f.readText()) }.getOrNull()
            if (manifest != null && manifest.name in targetToolIds) {
                if (manifest.owner != null && manifest.owner != TOOLCHAIN_OWNER) {
                    conflicts.add(manifest.name)
                }
            }
        }
        return conflicts
    }

    public fun computeRegistrationSnapshot(targetOutputs: List<InstallManifestOutput>): List<RegSnapshotEntry> {
        val snapshot = mutableListOf<RegSnapshotEntry>()
        val targetToolIds = targetOutputs.map { it.toolId }.toSet()
        val files = pluginsDir.listFiles { _, name -> name.endsWith(".json") }.orEmpty()

        for (output in targetOutputs) {
            val f = File(pluginsDir, "${output.toolId}.json")
            if (f.exists()) {
                val manifest = runCatching { json.decodeFromString<ToolManifest>(f.readText()) }.getOrNull()
                if (manifest?.owner == TOOLCHAIN_OWNER) {
                    snapshot.add(RegSnapshotEntry(output.toolId, f.readText()))
                } else {
                    snapshot.add(RegSnapshotEntry(output.toolId, "ABSENT"))
                }
            } else {
                snapshot.add(RegSnapshotEntry(output.toolId, "ABSENT"))
            }
        }

        // Owned manifests present before activation that would be removed by activation
        for (f in files) {
            val manifest = runCatching { json.decodeFromString<ToolManifest>(f.readText()) }.getOrNull()
            if (manifest != null && manifest.owner == TOOLCHAIN_OWNER && manifest.name !in targetToolIds) {
                snapshot.add(RegSnapshotEntry(manifest.name, f.readText()))
            }
        }
        return snapshot
    }

    public fun applyOwnedRegistrations(targetOutputs: List<InstallManifestOutput>, binDir: File) {
        val targetToolIds = targetOutputs.map { it.toolId }.toSet()
        val files = pluginsDir.listFiles { _, name -> name.endsWith(".json") }.orEmpty()

        // Remove owned registrations absent from target outputs (never touch foreign ones)
        for (f in files) {
            val manifest = runCatching { json.decodeFromString<ToolManifest>(f.readText()) }.getOrNull()
            if (manifest != null && manifest.owner == TOOLCHAIN_OWNER && manifest.name !in targetToolIds) {
                f.delete()
            }
        }

        // Write owned registrations for target outputs
        for (output in targetOutputs) {
            val f = File(pluginsDir, "${output.toolId}.json")
            val manifest = ToolManifest(
                name = output.toolId,
                executable = File(binDir, output.file).absolutePath.replace("\\", "/"),
                description = "Published binary for ${output.toolId}",
                owner = TOOLCHAIN_OWNER
            )
            writeAtomically(f, json.encodeToString(manifest))
        }
    }

    public fun restoreRegistrationSnapshot(snapshot: List<RegSnapshotEntry>) {
        for (entry in snapshot) {
            val f = File(pluginsDir, "${entry.toolId}.json")
            if (entry.previous == "ABSENT") {
                if (f.exists()) f.delete()
            } else {
                writeAtomically(f, entry.previous)
            }
        }
    }

    private fun writeAtomically(file: File, content: String) {
        file.parentFile?.mkdirs()
        val tmp = File(file.parentFile, "${file.name}.${System.nanoTime()}.tmp")
        FileOutputStream(tmp).use { fos ->
            fos.write(content.toByteArray(Charsets.UTF_8))
            fos.flush()
            fos.fd.sync()
        }
        try {
            Files.move(
                tmp.toPath(),
                file.toPath(),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
            )
        } catch (_: Exception) {
            Files.move(
                tmp.toPath(),
                file.toPath(),
                StandardCopyOption.REPLACE_EXISTING
            )
        }
    }
}
