package io.ltirom.server.toolchain

import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonPrimitive
import java.io.File
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.zip.CRC32

public enum class JournalEntryType {
    REQUEST,
    REG_SNAPSHOT,
    LINK_INTENT,
    RESTORE_STARTED,
    LINK_SWAPPED,
    REGS_APPLIED,
    VERIFIED,
    COMMITTED,
    RESTORE_LINKED,
    RESTORED,
    MAINTENANCE_FAILED,
    REJECTED
}

public enum class ActivationState {
    SWITCHING,
    COMMITTED,
    RESTORED,
    REJECTED,
    MAINTENANCE_FAILED
}

@Serializable
public data class RegSnapshotEntry(
    val toolId: String,
    val previous: String
)

@Serializable
public data class ActivationRecord(
    val requestId: String,
    val requestHash: String,
    var from: String?,
    var to: String?,
    var state: ActivationState,
    var rejectionCode: String? = null,
    var resolvedBy: String? = null,
    var lastSeq: Long = 0L,
    var activeInstallId: String? = null
)

public data class InFlightTransaction(
    val requestId: String,
    val requestHash: String,
    val from: String?,
    val to: String?,
    val lastIntentType: JournalEntryType,
    val hasLinkSwapped: Boolean = false,
    val hasRestoreLinked: Boolean = false,
    val regSnapshot: List<RegSnapshotEntry> = emptyList()
)

@Serializable
public data class ToolchainStateProjection(
    val state: ToolchainState,
    val activeInstallId: String?,
    val previousInstallId: String?,
    val recoveryRequiredBy: String?
)

public data class JournalFoldResult(
    val state: ToolchainState,
    val activeInstallId: String?,
    val previousInstallId: String?,
    val recoveryRequiredBy: String?,
    val inFlightTransaction: InFlightTransaction?,
    val activations: Map<String, ActivationRecord>,
    val nextSeq: Long
)

public class ToolchainJournal(
    private val stateDir: File
) {
    private val journalFile = File(stateDir, "toolchain-journal.jsonl")
    private val activationsDir = File(stateDir, "activations")
    private val stateFile = File(stateDir, "toolchain-state.json")
    private val json = Json { ignoreUnknownKeys = true; prettyPrint = false }
    private val lock = Any()

    init {
        stateDir.mkdirs()
        activationsDir.mkdirs()
    }

    public fun append(
        requestId: String,
        type: JournalEntryType,
        payload: String
    ): Long = synchronized(lock) {
        val currentFold = foldInternal(rewriteProjections = false)
        val seq = currentFold.nextSeq
        val crc = computeCrc32(seq, requestId, type, payload)

        val entryJson = json.encodeToString(
            RawJournalEntry(
                seq = seq,
                requestId = requestId,
                type = type.name,
                payload = payload,
                crc32 = crc
            )
        )

        FileOutputStream(journalFile, true).use { fos ->
            fos.write((entryJson + "\n").toByteArray(Charsets.UTF_8))
            fos.flush()
            fos.fd.sync()
        }

        // Fold and write projections for modified record
        val updatedFold = foldInternal(rewriteProjections = false)
        val stateProjection = ToolchainStateProjection(
            state = updatedFold.state,
            activeInstallId = updatedFold.activeInstallId,
            previousInstallId = updatedFold.previousInstallId,
            recoveryRequiredBy = updatedFold.recoveryRequiredBy
        )
        writeAtomically(stateFile, json.encodeToString(stateProjection))
        val modifiedRecord = updatedFold.activations[requestId]
        if (modifiedRecord != null) {
            val actFile = File(activationsDir, "$requestId.json")
            writeAtomically(actFile, json.encodeToString(modifiedRecord))
        }
        seq
    }

    public fun fold(): JournalFoldResult = synchronized(lock) {
        foldInternal(rewriteProjections = false)
    }

    public fun foldWithProjections(): JournalFoldResult = synchronized(lock) {
        foldInternal(rewriteProjections = true)
    }

    public fun getRecord(requestId: String): ActivationRecord? = fold().activations[requestId]

    public fun findLastEntryForRequest(requestId: String): JournalEntryType? = synchronized(lock) {
        if (!journalFile.exists()) return null
        var lastType: JournalEntryType? = null
        journalFile.forEachLine { line ->
            if (line.isNotBlank()) {
                val entry = parseLine(line)
                if (entry != null && entry.requestId == requestId) {
                    val type = runCatching { JournalEntryType.valueOf(entry.type) }.getOrNull()
                    if (type != null) {
                        lastType = type
                    }
                }
            }
        }
        lastType
    }

    private fun foldInternal(rewriteProjections: Boolean): JournalFoldResult {
        if (!journalFile.exists()) {
            val emptyResult = JournalFoldResult(
                state = ToolchainState.UNINITIALIZED,
                activeInstallId = null,
                previousInstallId = null,
                recoveryRequiredBy = null,
                inFlightTransaction = null,
                activations = emptyMap(),
                nextSeq = 1L
            )
            if (rewriteProjections || !stateFile.exists()) writeProjections(emptyResult)
            return emptyResult
        }

        var activeInstallId: String? = null
        var previousInstallId: String? = null
        val activations = mutableMapOf<String, ActivationRecord>()
        var inFlight: InFlightTransaction? = null
        var lastMaintenanceFailedReqId: String? = null
        var latestMaintenanceFailedSeq: Long = -1L
        var latestCommittedSeq: Long = -1L
        val restoredMaintenanceReqIds = mutableSetOf<String>()
        var highestSeq: Long = 0L
        var hasCorruptedNonTailEntry = false

        val lines = journalFile.readLines().filter { it.isNotBlank() }
        lines.forEachIndexed { index, line ->
            val rawEntry = parseLine(line)
            val isLastLine = index == lines.size - 1
            if (rawEntry == null) {
                if (!isLastLine) {
                    hasCorruptedNonTailEntry = true
                }
            } else {
                val entryType = runCatching { JournalEntryType.valueOf(rawEntry.type) }.getOrNull()
                if (entryType != null) {
                    highestSeq = maxOf(highestSeq, rawEntry.seq)
                    when (entryType) {
                        JournalEntryType.REQUEST -> {
                            val parsed = runCatching { json.decodeFromString<JsonObject>(rawEntry.payload) }.getOrNull()
                            val from = parsed?.get("from")?.jsonPrimitive?.contentOrNull
                            val to = parsed?.get("to")?.jsonPrimitive?.contentOrNull
                            val requestHash = parsed?.get("requestHash")?.jsonPrimitive?.contentOrNull ?: ""
                            inFlight = InFlightTransaction(
                                requestId = rawEntry.requestId,
                                requestHash = requestHash,
                                from = from,
                                to = to,
                                lastIntentType = JournalEntryType.REQUEST
                            )
                            activations[rawEntry.requestId] = ActivationRecord(
                                requestId = rawEntry.requestId,
                                requestHash = requestHash,
                                from = from,
                                to = to,
                                state = ActivationState.SWITCHING,
                                lastSeq = rawEntry.seq
                            )
                        }
                        JournalEntryType.REG_SNAPSHOT -> {
                            if (inFlight?.requestId == rawEntry.requestId) {
                                val snapshots = runCatching {
                                    json.decodeFromString<List<RegSnapshotEntry>>(rawEntry.payload)
                                }.getOrDefault(emptyList())
                                inFlight = inFlight?.copy(
                                    lastIntentType = JournalEntryType.REG_SNAPSHOT,
                                    regSnapshot = snapshots
                                )
                            }
                        }
                        JournalEntryType.LINK_INTENT -> {
                            if (inFlight?.requestId == rawEntry.requestId) {
                                inFlight = inFlight?.copy(lastIntentType = JournalEntryType.LINK_INTENT)
                            }
                        }
                        JournalEntryType.RESTORE_STARTED -> {
                            if (inFlight?.requestId == rawEntry.requestId) {
                                inFlight = inFlight?.copy(lastIntentType = JournalEntryType.RESTORE_STARTED)
                            }
                        }
                        JournalEntryType.LINK_SWAPPED -> {
                            if (inFlight?.requestId == rawEntry.requestId) {
                                inFlight = inFlight?.copy(hasLinkSwapped = true)
                            }
                        }
                        JournalEntryType.REGS_APPLIED -> {}
                        JournalEntryType.VERIFIED -> {}
                        JournalEntryType.COMMITTED -> {
                            val tx = inFlight
                            inFlight = null
                            val parsed = runCatching { json.decodeFromString<JsonObject>(rawEntry.payload) }.getOrNull()
                            val committedId = parsed?.get("activeInstallId")?.jsonPrimitive?.contentOrNull
                                ?: tx?.to
                            val reqHash = parsed?.get("requestHash")?.jsonPrimitive?.contentOrNull ?: tx?.requestHash ?: ""
                            previousInstallId = tx?.from ?: activeInstallId
                            activeInstallId = committedId
                            latestCommittedSeq = rawEntry.seq
                            val record = activations.getOrPut(rawEntry.requestId) {
                                ActivationRecord(
                                    requestId = rawEntry.requestId,
                                    requestHash = reqHash,
                                    from = tx?.from,
                                    to = committedId,
                                    state = ActivationState.COMMITTED
                                )
                            }
                            record.state = ActivationState.COMMITTED
                            record.activeInstallId = committedId
                            record.lastSeq = rawEntry.seq
                            if (record.from == null && tx?.from != null) record.from = tx.from
                            if (record.to == null) record.to = committedId
                            if (lastMaintenanceFailedReqId != null) {
                                activations[lastMaintenanceFailedReqId]?.resolvedBy = rawEntry.requestId
                                lastMaintenanceFailedReqId = null
                            }
                        }
                        JournalEntryType.RESTORE_LINKED -> {
                            if (inFlight?.requestId == rawEntry.requestId) {
                                inFlight = inFlight?.copy(hasRestoreLinked = true)
                            }
                        }
                        JournalEntryType.RESTORED -> {
                            val tx = inFlight
                            inFlight = null
                            val parsed = runCatching { json.decodeFromString<JsonObject>(rawEntry.payload) }.getOrNull()
                            val restoredId = parsed?.get("activeInstallId")?.jsonPrimitive?.contentOrNull
                                ?: tx?.from
                            val reqHash = parsed?.get("requestHash")?.jsonPrimitive?.contentOrNull ?: tx?.requestHash ?: ""
                            activeInstallId = restoredId
                            val record = activations.getOrPut(rawEntry.requestId) {
                                ActivationRecord(
                                    requestId = rawEntry.requestId,
                                    requestHash = reqHash,
                                    from = tx?.from,
                                    to = tx?.to,
                                    state = ActivationState.RESTORED
                                )
                            }
                            record.state = ActivationState.RESTORED
                            record.activeInstallId = restoredId
                            record.lastSeq = rawEntry.seq
                            if (record.from == null && tx?.from != null) record.from = tx.from
                            if (record.to == null && tx?.to != null) record.to = tx.to
                            if (lastMaintenanceFailedReqId == rawEntry.requestId) {
                                activations[rawEntry.requestId]?.resolvedBy = rawEntry.requestId
                                lastMaintenanceFailedReqId = null
                                restoredMaintenanceReqIds.add(rawEntry.requestId)
                            }
                        }
                        JournalEntryType.MAINTENANCE_FAILED -> {
                            val tx = inFlight
                            inFlight = null
                            val parsed = runCatching { json.decodeFromString<JsonObject>(rawEntry.payload) }.getOrNull()
                            val reqHash = parsed?.get("requestHash")?.jsonPrimitive?.contentOrNull ?: tx?.requestHash ?: ""
                            lastMaintenanceFailedReqId = rawEntry.requestId
                            latestMaintenanceFailedSeq = rawEntry.seq
                            val record = activations.getOrPut(rawEntry.requestId) {
                                ActivationRecord(
                                    requestId = rawEntry.requestId,
                                    requestHash = reqHash,
                                    from = tx?.from,
                                    to = tx?.to,
                                    state = ActivationState.MAINTENANCE_FAILED
                                )
                            }
                            record.state = ActivationState.MAINTENANCE_FAILED
                            record.lastSeq = rawEntry.seq
                            if (record.from == null && tx?.from != null) record.from = tx.from
                            if (record.to == null && tx?.to != null) record.to = tx.to
                        }
                        JournalEntryType.REJECTED -> {
                            val tx = inFlight
                            inFlight = null
                            val parsed = runCatching { json.decodeFromString<JsonObject>(rawEntry.payload) }.getOrNull()
                            val code = parsed?.get("code")?.jsonPrimitive?.contentOrNull
                            val reqHash = parsed?.get("requestHash")?.jsonPrimitive?.contentOrNull ?: tx?.requestHash ?: ""
                            val record = activations.getOrPut(rawEntry.requestId) {
                                ActivationRecord(
                                    requestId = rawEntry.requestId,
                                    requestHash = reqHash,
                                    from = tx?.from,
                                    to = tx?.to,
                                    state = ActivationState.REJECTED
                                )
                            }
                            record.state = ActivationState.REJECTED
                            record.rejectionCode = code
                            record.lastSeq = rawEntry.seq
                            if (record.from == null && tx?.from != null) record.from = tx.from
                            if (record.to == null && tx?.to != null) record.to = tx.to
                        }
                    }
                }
            }
        }

        val derivedRecoveryRequiredBy = if (
            lastMaintenanceFailedReqId != null &&
            latestCommittedSeq < latestMaintenanceFailedSeq &&
            lastMaintenanceFailedReqId !in restoredMaintenanceReqIds
        ) {
            lastMaintenanceFailedReqId
        } else {
            null
        }

        val derivedState = when {
            hasCorruptedNonTailEntry -> ToolchainState.RECOVERY_REQUIRED
            derivedRecoveryRequiredBy != null -> ToolchainState.RECOVERY_REQUIRED
            activeInstallId != null -> ToolchainState.READY
            else -> ToolchainState.UNINITIALIZED
        }

        val result = JournalFoldResult(
            state = derivedState,
            activeInstallId = activeInstallId,
            previousInstallId = previousInstallId,
            recoveryRequiredBy = derivedRecoveryRequiredBy,
            inFlightTransaction = inFlight,
            activations = activations,
            nextSeq = highestSeq + 1
        )

        val stateProjectionMissing = !stateFile.exists()
        if (rewriteProjections || stateProjectionMissing) {
            writeProjections(result)
        }
        return result
    }

    private fun parseLine(line: String): RawJournalEntry? {
        val entry = runCatching { json.decodeFromString<RawJournalEntry>(line) }.getOrNull() ?: return null
        val type = runCatching { JournalEntryType.valueOf(entry.type) }.getOrNull() ?: return null
        val expectedCrc = computeCrc32(entry.seq, entry.requestId, type, entry.payload)
        return if (entry.crc32 == expectedCrc) entry else null
    }

    private fun writeProjections(result: JournalFoldResult) {
        val stateProjection = ToolchainStateProjection(
            state = result.state,
            activeInstallId = result.activeInstallId,
            previousInstallId = result.previousInstallId,
            recoveryRequiredBy = result.recoveryRequiredBy
        )
        writeAtomically(stateFile, json.encodeToString(stateProjection))

        for ((id, record) in result.activations) {
            val actFile = File(activationsDir, "$id.json")
            writeAtomically(actFile, json.encodeToString(record))
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

    private fun computeCrc32(
        seq: Long,
        requestId: String,
        type: JournalEntryType,
        payload: String
    ): Long {
        val crc = CRC32()
        crc.update("$seq:$requestId:${type.name}:$payload".toByteArray(Charsets.UTF_8))
        return crc.value
    }
}

@Serializable
internal data class RawJournalEntry(
    val seq: Long,
    val requestId: String,
    val type: String,
    val payload: String,
    val crc32: Long
)
