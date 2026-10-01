package io.ltirom.server.infra

import io.ltirom.server.model.PersistedRunMetadata
import io.ltirom.server.model.RunStatusValue
import io.ltirom.server.model.SequencedStreamEvent
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.File
import java.util.concurrent.ConcurrentHashMap

public class RunJournalStore(
    private val baseDir: File = File(System.getenv("HOME") ?: System.getProperty("user.home"), ".ltirom/runs"),
    private val json: Json = Json { ignoreUnknownKeys = true; encodeDefaults = true },
    private val isPidAlive: (Long) -> Boolean = { pid ->
        ProcessHandle.of(pid).map { it.isAlive }.orElse(false)
    }
) {
    init {
        if (!baseDir.exists()) {
            baseDir.mkdirs()
        }
    }

    private val runLocks = ConcurrentHashMap<String, Any>()

    private fun getLock(runId: String): Any = runLocks.computeIfAbsent(runId) { Any() }

    public fun getRunDir(runId: String): File = File(baseDir, runId)

    public fun recordRun(metadata: PersistedRunMetadata) {
        synchronized(getLock(metadata.runId)) {
            val dir = getRunDir(metadata.runId)
            dir.mkdirs()
            val runFile = File(dir, "run.json")
            val content = json.encodeToString(metadata)
            runFile.writeText(content)
        }
    }

    public fun updateRunMetadata(runId: String, update: (PersistedRunMetadata) -> PersistedRunMetadata): PersistedRunMetadata? {
        return synchronized(getLock(runId)) {
            val current = loadRun(runId) ?: return@synchronized null
            val updated = update(current)
            val dir = getRunDir(runId)
            dir.mkdirs()
            val runFile = File(dir, "run.json")
            runFile.writeText(json.encodeToString(updated))
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
                val updated = current.copy(lastSeq = event.seq)
                val runFile = File(dir, "run.json")
                runFile.writeText(json.encodeToString(updated))
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

    public fun loadRun(runId: String): PersistedRunMetadata? {
        val dir = getRunDir(runId)
        val runFile = File(dir, "run.json")
        if (!runFile.exists()) return null
        return runCatching {
            json.decodeFromString<PersistedRunMetadata>(runFile.readText())
        }.getOrNull()
    }

    public fun loadAllRuns(): List<PersistedRunMetadata> {
        if (!baseDir.exists() || !baseDir.isDirectory) return emptyList()
        val dirs = baseDir.listFiles { f -> f.isDirectory } ?: return emptyList()
        return dirs.mapNotNull { dir ->
            val runFile = File(dir, "run.json")
            if (runFile.exists()) {
                runCatching { json.decodeFromString<PersistedRunMetadata>(runFile.readText()) }.getOrNull()
            } else null
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
                val alive = run.pid?.let { isPidAlive(it) } ?: false
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
