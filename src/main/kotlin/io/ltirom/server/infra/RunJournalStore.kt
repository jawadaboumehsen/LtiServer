package io.ltirom.server.infra

import io.ltirom.server.model.PersistedRunMetadata
import io.ltirom.server.model.RunStatusValue
import io.ltirom.server.model.SequencedStreamEvent
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.concurrent.ConcurrentHashMap

public class RunJournalStore(
    private val baseDir: File = File(System.getenv("HOME") ?: System.getProperty("user.home"), ".ltirom/runs"),
    private val json: Json = Json { ignoreUnknownKeys = true; encodeDefaults = true },
    private val isPidAlive: (Long) -> Boolean = { pid ->
        ProcessHandle.of(pid).map { it.isAlive }.orElse(false)
    },
    /** Whether the process now behind a pid is the one that started at the recorded time (not a reused pid). */
    private val startMatches: (Long, Long?) -> Boolean = { pid, recordedStart ->
        val actual = ProcessHandle.of(pid).flatMap { it.info().startInstant() }.map { it.toEpochMilli() }
        recordedStart == null || !actual.isPresent || kotlin.math.abs(actual.get() - recordedStart) <= START_TOLERANCE_MS
    }
) {
    init {
        if (!baseDir.exists()) {
            baseDir.mkdirs()
        }
    }

    private val runLocks = ConcurrentHashMap<String, Any>()

    private fun getLock(runId: String): Any = runLocks.computeIfAbsent(runId) { Any() }

    public fun getRunDir(runId: String): File {
        val dir = File(baseDir, runId)
        require(dir.canonicalFile.parentFile == baseDir.canonicalFile) { "Invalid run id '${runId.take(40)}'" }
        return dir
    }

    /**
     * Replaces run.json in one step. Overwriting it in place left a truncated file after a crash or a full
     * disk, and an unreadable record is a run the service can no longer account for.
     */
    private fun writeMetadata(dir: File, metadata: PersistedRunMetadata) {
        val target = File(dir, "run.json")
        val temp = File(dir, "run.json.tmp")
        temp.writeText(json.encodeToString(metadata))
        Files.move(temp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    }

    private fun readMetadata(runFile: File): PersistedRunMetadata? = try {
        json.decodeFromString<PersistedRunMetadata>(runFile.readText())
    } catch (e: Exception) {
        // Not "no such run": say which record is damaged so it can be looked at.
        System.err.println("RunJournalStore: unreadable run record ${runFile.path}: ${e.message}")
        null
    }

    public fun recordRun(metadata: PersistedRunMetadata) {
        synchronized(getLock(metadata.runId)) {
            val dir = getRunDir(metadata.runId)
            dir.mkdirs()
            writeMetadata(dir, metadata)
        }
    }

    public fun updateRunMetadata(runId: String, update: (PersistedRunMetadata) -> PersistedRunMetadata): PersistedRunMetadata? {
        return synchronized(getLock(runId)) {
            val current = loadRun(runId) ?: return@synchronized null
            val updated = update(current)
            val dir = getRunDir(runId)
            dir.mkdirs()
            writeMetadata(dir, updated)
            updated
        }
    }

    public fun appendEvent(runId: String, event: SequencedStreamEvent) {
        synchronized(getLock(runId)) {
            val dir = getRunDir(runId)
            dir.mkdirs()
            val eventsFile = File(dir, "events.jsonl")
            val line = json.encodeToString(event) + "\n"
            eventsFile.appendText(line)

            val current = loadRun(runId)
            if (current != null && event.seq > current.lastSeq) {
                writeMetadata(dir, current.copy(lastSeq = event.seq))
            }
        }
    }

    public fun replay(runId: String, fromSeq: Long = 0L): List<SequencedStreamEvent> {
        val dir = getRunDir(runId)
        val eventsFile = File(dir, "events.jsonl")
        if (!eventsFile.exists()) return emptyList()

        val results = mutableListOf<SequencedStreamEvent>()
        eventsFile.bufferedReader().useLines { lines ->
            for (line in lines) {
                val trimmed = line.trim()
                if (trimmed.isEmpty()) continue
                val decoded = runCatching { json.decodeFromString<SequencedStreamEvent>(trimmed) }.getOrNull()
                if (decoded != null && decoded.seq >= fromSeq) {
                    results.add(decoded)
                }
            }
        }
        return results
    }

    /**
     * At most [limit] events with seq >= [fromSeq], reading the file from byte [startOffset]. The returned offset is
     * just past the last line consumed, so the next call continues there instead of scanning the file again. A
     * trailing line that is not complete yet (being written) is left for the next call.
     */
    public fun readPage(
        runId: String,
        fromSeq: Long,
        startOffset: Long = 0L,
        limit: Int,
    ): io.ltirom.server.session.ReplayPage {
        val file = File(getRunDir(runId), "events.jsonl")
        if (!file.exists()) return io.ltirom.server.session.ReplayPage(emptyList(), startOffset)
        val events = ArrayList<SequencedStreamEvent>()
        var offset = startOffset
        java.io.RandomAccessFile(file, "r").use { raf ->
            raf.seek(startOffset.coerceIn(0L, raf.length()))
            val input = java.nio.channels.Channels.newInputStream(raf.channel)
            val chunk = ByteArray(READ_CHUNK_BYTES)
            val line = java.io.ByteArrayOutputStream()
            read@ while (events.size < limit) {
                val count = input.read(chunk)
                if (count < 0) break
                for (i in 0 until count) {
                    if (chunk[i] != NEWLINE) {
                        line.write(chunk[i].toInt())
                        continue
                    }
                    offset += line.size() + 1
                    val decoded = line.toString(Charsets.UTF_8).trim().takeIf { it.isNotEmpty() }
                        ?.let { text -> runCatching { json.decodeFromString<SequencedStreamEvent>(text) }.getOrNull() }
                    line.reset()
                    if (decoded != null && decoded.seq >= fromSeq) events.add(decoded)
                    if (events.size >= limit) break@read
                }
            }
        }
        return io.ltirom.server.session.ReplayPage(events, offset)
    }

    public fun loadRun(runId: String): PersistedRunMetadata? {
        val dir = getRunDir(runId)
        val runFile = File(dir, "run.json")
        if (!runFile.exists()) return null
        return readMetadata(runFile)
    }

    public fun loadAllRuns(): List<PersistedRunMetadata> {
        if (!baseDir.exists() || !baseDir.isDirectory) return emptyList()
        val dirs = baseDir.listFiles { f -> f.isDirectory } ?: return emptyList()
        return dirs.mapNotNull { dir ->
            val runFile = File(dir, "run.json")
            if (runFile.exists()) readMetadata(runFile) else null
        }
    }

    /**
     * Rehydrates persisted runs on daemon startup:
     * Any run recorded as QUEUED, RUNNING or CANCELLING whose PID is no longer alive is transitioned to
     * INTERRUPTED (a QUEUED run of a dead service would otherwise count as active forever).
     */
    public fun rehydrateRuns(): List<PersistedRunMetadata> {
        val runs = loadAllRuns()
        val rehydrated = mutableListOf<PersistedRunMetadata>()
        for (run in runs) {
            if (run.status == RunStatusValue.QUEUED || run.status == RunStatusValue.RUNNING ||
                run.status == RunStatusValue.CANCELLING
            ) {
                val alive = run.pid?.let { isPidAlive(it) && startMatches(it, run.pidStartedAtEpochMs) } ?: false
                if (!alive) {
                    val interrupted = run.copy(
                        status = RunStatusValue.INTERRUPTED,
                        endedAtEpochMs = System.currentTimeMillis()
                    )
                    recordRun(interrupted)
                    rehydrated.add(interrupted)
                    continue
                }
            }
            rehydrated.add(run)
        }
        return rehydrated
    }

    /**
     * Prunes terminal runs older than 30 days.
     */
    public fun pruneOldRuns(retentionMs: Long = 30L * 24 * 3600 * 1000L, now: Long = System.currentTimeMillis()): Int {
        val runs = loadAllRuns()
        var pruned = 0
        val cutoff = now - retentionMs
        for (run in runs) {
            val isTerminal = run.status in setOf(
                RunStatusValue.COMPLETED,
                RunStatusValue.FAILED,
                RunStatusValue.CANCELLED,
                RunStatusValue.INTERRUPTED
            )
            if (isTerminal) {
                val timestamp = run.endedAtEpochMs ?: run.startedAtEpochMs
                if (timestamp < cutoff) {
                    val dir = getRunDir(run.runId)
                    if (dir.deleteRecursively()) {
                        pruned++
                    }
                }
            }
        }
        return pruned
    }
}

private const val START_TOLERANCE_MS = 2_000L

private const val READ_CHUNK_BYTES = 64 * 1024
private const val NEWLINE = '\n'.code.toByte()
