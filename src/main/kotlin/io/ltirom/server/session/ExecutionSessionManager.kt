package io.ltirom.server.session

import io.ltirom.server.infra.RunJournalStore
import io.ltirom.server.model.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.asSharedFlow
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicLong

/**
 * Ring buffer of the latest events, bounded by count *and* by bytes (output lines can be large), whose sequence
 * numbers are handed out and published under one lock: event N+1 can never become visible before event N, so a
 * reader that follows the numbers can never skip one.
 */
public class CircularEventBuffer(
    public val capacity: Int = 10_000,
    initialSeq: Long = 0L,
    public val maxBytes: Long = DEFAULT_MAX_BYTES,
) {
    private class Entry(val event: SequencedStreamEvent, val bytes: Long)

    private val entries = java.util.ArrayDeque<Entry>()
    private var nextSeq = initialSeq
    private var totalBytes = 0L

    @Synchronized
    public fun append(event: StreamEvent): SequencedStreamEvent {
        val sequenced = SequencedStreamEvent(nextSeq++, event)
        val bytes = estimateBytes(event)
        entries.addLast(Entry(sequenced, bytes))
        totalBytes += bytes
        // Always keeps the newest event, however large.
        while (entries.size > 1 && (entries.size > capacity || totalBytes > maxBytes)) {
            totalBytes -= entries.removeFirst().bytes
        }
        return sequenced
    }

    @Synchronized
    public fun replay(fromSeq: Long = 0L): List<SequencedStreamEvent> =
        entries.filter { it.event.seq >= fromSeq }.map { it.event }

    @Synchronized
    public fun floorSeq(): Long? = entries.peekFirst()?.event?.seq

    @Synchronized
    public fun lastSeq(): Long? = entries.peekLast()?.event?.seq

    @Synchronized
    public fun retainedBytes(): Long = totalBytes

    private fun estimateBytes(event: StreamEvent): Long = when (event) {
        is StreamEvent.OutputChunk -> event.text.length * BYTES_PER_CHAR + OVERHEAD_BYTES
        is StreamEvent.ExecutionFinished -> (event.summary?.length ?: 0) * BYTES_PER_CHAR + OVERHEAD_BYTES
        else -> OVERHEAD_BYTES
    }

    public companion object {
        public const val DEFAULT_MAX_BYTES: Long = 64L * 1024 * 1024
        private const val BYTES_PER_CHAR = 2L
        private const val OVERHEAD_BYTES = 64L
    }
}

/**
 * Headless background execution session preserving output streams across client reattachments.
 */
public class ExecutionSession(
    public val sessionId: String,
    public val toolId: String,
    public val arguments: List<String>,
    public val startTimeEpochMs: Long = System.currentTimeMillis(),
    bufferCapacity: Int = 10_000,
    private val journalStore: RunJournalStore? = null,
    initialSeq: Long = 0L
) {
    public val ringBuffer: CircularEventBuffer = CircularEventBuffer(bufferCapacity, initialSeq)
    private val appendLock = Any()
    private val _status = MutableStateFlow(SessionStatus.RUNNING)
    public val status = _status.asStateFlow()

    public var exitCode: Int? = null
        private set
    public var durationMs: Long? = null
        private set

    private val _liveEvents = kotlinx.coroutines.flow.MutableSharedFlow<SequencedStreamEvent>(extraBufferCapacity = 1000)
    public val liveEvents: kotlinx.coroutines.flow.SharedFlow<SequencedStreamEvent> = _liveEvents.asSharedFlow()

    /**
     * Numbered, stored, journaled and announced as one step. Output, completion and cancellation arrive on
     * different threads; without this their events could be stored in a different order than they were numbered.
     */
    public fun appendEvent(event: StreamEvent): SequencedStreamEvent = synchronized(appendLock) {
        if (event is StreamEvent.ExecutionFinished) {
            exitCode = event.exitCode
            durationMs = event.durationMs
            if (_status.value != SessionStatus.CANCELLED) {
                _status.value = if (event.exitCode == 0) SessionStatus.COMPLETED else SessionStatus.FAILED
            }
        }
        val sequenced = ringBuffer.append(event)
        try {
            journalStore?.appendEvent(sessionId, sequenced)
        } catch (e: java.io.IOException) {
            // The event still reaches attached clients; only its on-disk copy is missing.
            System.err.println("ExecutionSession $sessionId: could not journal event ${sequenced.seq}: ${e.message}")
        }
        _liveEvents.tryEmit(sequenced)
        sequenced
    }

    public fun markCancelled() {
        _status.value = SessionStatus.CANCELLED
    }

    public fun replay(fromSeq: Long = 0L): List<SequencedStreamEvent> {
        val floor = ringBuffer.floorSeq()
        if (journalStore != null && (floor == null || fromSeq < floor)) {
            val stored = journalStore.replay(sessionId, fromSeq)
            return if (stored.isEmpty() && floor != null) retainedAfterGap(fromSeq, floor, Int.MAX_VALUE) else stored
        }
        return ringBuffer.replay(fromSeq)
    }

    /**
     * One bounded piece of history from [fromSeq]; [journalOffset] lets the next piece continue where this ended.
     * If the journal has nothing for a start the buffer has already moved past (its writes failed), the page
     * resumes from what the buffer still holds and says where it skipped to, instead of being empty: an empty page
     * makes a reader wait for events - including the final one - that are already here.
     */
    public fun replayPage(fromSeq: Long, journalOffset: Long = 0L, limit: Int = REPLAY_PAGE_EVENTS): ReplayPage {
        val floor = ringBuffer.floorSeq()
        if (journalStore != null && (floor == null || fromSeq < floor)) {
            val page = journalStore.readPage(sessionId, fromSeq, journalOffset, limit)
            if (page.events.isEmpty() && floor != null) {
                return ReplayPage(retainedAfterGap(fromSeq, floor, limit), 0L, skippedToSeq = floor)
            }
            return page
        }
        return ReplayPage(ringBuffer.replay(fromSeq).take(limit), 0L)
    }

    private fun retainedAfterGap(fromSeq: Long, floor: Long, limit: Int): List<SequencedStreamEvent> {
        System.err.println(
            "ExecutionSession $sessionId: history from seq $fromSeq is not on disk; continuing from seq $floor"
        )
        return ringBuffer.replay(floor).take(limit)
    }

    /** All history from [fromSeq], read a page at a time: a long log is never held in memory whole. */
    public fun replayFlow(fromSeq: Long): kotlinx.coroutines.flow.Flow<SequencedStreamEvent> =
        kotlinx.coroutines.flow.flow {
            var next = fromSeq
            var offset = 0L
            while (true) {
                val page = replayPage(next, offset)
                if (page.events.isEmpty()) break
                page.events.forEach { emit(it) }
                next = page.events.last().seq + 1
                offset = page.journalOffset
            }
        }

    /**
     * Every event from [fromSeq] on, ending after ExecutionFinished. Events are read by sequence number, a page at
     * a time, from the buffer or the journal; the live flow only signals "something new". (Reading the events from
     * it lost them for a slow reader, and a lost ExecutionFinished left the client waiting for the time limit.)
     */
    public fun attach(fromSeq: Long = 0L): kotlinx.coroutines.flow.Flow<SequencedStreamEvent> =
        kotlinx.coroutines.flow.channelFlow {
            val wake = kotlinx.coroutines.channels.Channel<Unit>(kotlinx.coroutines.channels.Channel.CONFLATED)
            // Subscribed before the first read, so nothing appended after it goes unnoticed.
            val subscription = launch(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) {
                liveEvents.collect { wake.trySend(Unit) }
            }
            var next = fromSeq
            var offset = 0L
            try {
                while (true) {
                    val page = replayPage(next, offset)
                    for (event in page.events) {
                        if (event.seq < next) continue
                        send(event)
                        next = event.seq + 1
                        if (event.event is StreamEvent.ExecutionFinished) return@channelFlow
                    }
                    offset = page.journalOffset
                    if (page.events.isEmpty()) wake.receive()
                }
            } finally {
                subscription.cancel()
            }
        }

    public fun lastSeq(): Long? = ringBuffer.lastSeq() ?: journalStore?.loadRun(sessionId)?.lastSeq

    public fun toSummary(): SessionSummary = SessionSummary(
        sessionId = sessionId,
        toolId = toolId,
        arguments = arguments,
        startTimeEpochMs = startTimeEpochMs,
        status = _status.value,
        exitCode = exitCode,
        durationMs = durationMs
    )
}

/**
 * Central registry managing background execution sessions with replay and reattachment capabilities.
 */
public class ExecutionSessionManager(
    private val defaultCapacity: Int = 10_000,
    public val journalStore: RunJournalStore = RunJournalStore(),
    autoRehydrate: Boolean = false
) {
    private val sessions = ConcurrentHashMap<String, ExecutionSession>()
    private val runs = ConcurrentHashMap<String, ServerRun>()
    private val idempotencyMap = ConcurrentHashMap<String, Pair<RunHandle, Long>>()

    init {
        if (autoRehydrate) {
            rehydrate()
        }
    }

    public fun rehydrate(): List<PersistedRunMetadata> {
        val persisted = journalStore.rehydrateRuns()
        for (meta in persisted) {
            runs[meta.runId] = restoredRun(meta)
            val handle = RunHandle(runId = meta.runId, status = meta.status, startedAtEpochMs = meta.startedAtEpochMs)
            idempotencyMap[meta.idempotencyKey] = Pair(handle, meta.startedAtEpochMs)
        }
        pruneFinished()
        return persisted
    }

    /** A run rebuilt from its journal record; its output is read back from the journal when asked for. */
    private fun restoredRun(meta: PersistedRunMetadata): ServerRun {
        val session = ExecutionSession(
            sessionId = meta.runId,
            toolId = meta.request.request.toolId,
            arguments = meta.request.request.arguments,
            bufferCapacity = defaultCapacity,
            journalStore = journalStore,
            initialSeq = meta.lastSeq + 1L
        )
        return ServerRun(
            runId = meta.runId,
            request = meta.request,
            idempotencyKey = meta.idempotencyKey,
            workspaceLock = meta.workspaceLock,
            status = meta.status,
            startedAtEpochMs = meta.startedAtEpochMs,
            exitCode = meta.exitCode,
            pid = meta.pid,
            endedAtEpochMs = meta.endedAtEpochMs,
            session = session,
            journalStore = journalStore
        )
    }

    public fun createSession(
        toolId: String,
        arguments: List<String>,
        sessionId: String = java.util.UUID.randomUUID().toString().take(12),
        initialSeq: Long = 0L
    ): ExecutionSession {
        val session = ExecutionSession(
            sessionId = sessionId,
            toolId = toolId,
            arguments = arguments,
            bufferCapacity = defaultCapacity,
            journalStore = journalStore,
            initialSeq = initialSeq
        )
        sessions[sessionId] = session
        pruneFinished()
        return session
    }

    public fun getSession(sessionId: String): ExecutionSession? = sessions[sessionId] ?: runs[sessionId]?.session

    public fun listSessions(): List<SessionSummary> = sessions.values.map { it.toSummary() }

    public fun cleanupOldSessions(maxAgeMs: Long = 3600_000L) {
        val cutoff = System.currentTimeMillis() - maxAgeMs
        sessions.entries.removeIf { (_, session) ->
            session.status.value != SessionStatus.RUNNING && session.startTimeEpochMs < cutoff
        }
        journalStore.pruneOldRuns()
    }

    public fun findIdempotentRun(key: String, ttlMs: Long = 600_000L): RunHandle? {
        val entry = idempotencyMap[key] ?: return null
        val now = System.currentTimeMillis()
        return if (now - entry.second < ttlMs) entry.first else null
    }

    public fun findConflictingRun(workspaceLock: String): ServerRun? {
        return runs.values.find {
            it.workspaceLock == workspaceLock &&
            (it.status == RunStatusValue.QUEUED || it.status == RunStatusValue.RUNNING || it.status == RunStatusValue.CANCELLING)
        }
    }

    public fun activeRuns(): List<ServerRun> {
        return runs.values.filter {
            it.status == RunStatusValue.QUEUED || it.status == RunStatusValue.RUNNING || it.status == RunStatusValue.CANCELLING
        }
    }

    /**
     * Saves the run, then makes it visible. If saving fails nothing is registered, so no queued run without a
     * worker is left behind to count as active work or to answer a retry with the same key.
     */
    public fun registerRun(
        runId: String,
        request: StartRunRequest,
        idempotencyKey: String,
        workspaceLock: String?
    ): ServerRun {
        val startedAt = System.currentTimeMillis()
        journalStore.recordRun(
            PersistedRunMetadata(
                runId = runId,
                request = request,
                workingDirectory = request.request.workingDirectory ?: "",
                workspaceLock = workspaceLock,
                idempotencyKey = idempotencyKey,
                status = RunStatusValue.QUEUED,
                startedAtEpochMs = startedAt,
                lastSeq = 0L
            )
        )
        // Runs use 1-based monotonic sequence numbering
        val session = ExecutionSession(
            sessionId = runId,
            toolId = request.request.toolId,
            arguments = request.request.arguments,
            bufferCapacity = defaultCapacity,
            journalStore = journalStore,
            initialSeq = 1L
        )
        val run = ServerRun(
            runId = runId,
            request = request,
            idempotencyKey = idempotencyKey,
            workspaceLock = workspaceLock,
            status = RunStatusValue.QUEUED,
            startedAtEpochMs = startedAt,
            session = session,
            journalStore = journalStore
        )
        runs[runId] = run
        idempotencyMap[idempotencyKey] = Pair(RunHandle(runId = runId, status = RunStatusValue.QUEUED, startedAtEpochMs = startedAt), startedAt)
        pruneFinished()
        return run
    }

    /**
     * Forgets what no client can still need, so a long-lived service does not grow with every build: finished
     * runs beyond the newest [MAX_RETAINED_RUNS] once they are [RETAIN_FINISHED_MS] old, standalone sessions of the
     * same age, and idempotency keys past their window. Active work is never touched; the journal on disk is.
     */
    public fun pruneFinished(now: Long = System.currentTimeMillis()) {
        val finished = runs.values
            .filter { it.status in FINISHED }
            .sortedByDescending { it.endedAtEpochMs ?: it.startedAtEpochMs }
        for (run in finished.drop(MAX_RETAINED_RUNS)) {
            if (now - (run.endedAtEpochMs ?: run.startedAtEpochMs) > RETAIN_FINISHED_MS) {
                runs.remove(run.runId)
                sessions.remove(run.runId)
            }
        }
        sessions.entries.removeIf { (_, session) ->
            session.status.value != SessionStatus.RUNNING && now - session.startTimeEpochMs > RETAIN_FINISHED_MS
        }
        idempotencyMap.entries.removeIf { (_, entry) -> now - entry.second > IDEMPOTENCY_WINDOW_MS }
    }

    private companion object {
        /** Run ids come from URLs; only plain ones may name a folder of the journal. */
        val SAFE_RUN_ID = Regex("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")
        const val MAX_RETAINED_RUNS = 200
        const val RETAIN_FINISHED_MS = 3_600_000L
        const val IDEMPOTENCY_WINDOW_MS = 600_000L
        val FINISHED = setOf(
            RunStatusValue.COMPLETED,
            RunStatusValue.FAILED,
            RunStatusValue.CANCELLED,
            RunStatusValue.INTERRUPTED,
        )
    }

    /**
     * Saves which process the run is. Unlike a status update this must not fail quietly: a process whose identity
     * is not on disk cannot be found, adopted or stopped by a restarted service. Throws [java.io.IOException].
     */
    public fun recordProcessIdentity(runId: String, pid: Long, startedAtEpochMs: Long?) {
        val run = runs[runId] ?: return
        run.pid = pid
        journalStore.updateRunMetadata(runId) { current ->
            current.copy(status = run.status, pid = pid, pidStartedAtEpochMs = startedAtEpochMs ?: current.pidStartedAtEpochMs)
        } ?: throw java.io.IOException("the record of run $runId is missing or unreadable")
    }

    public fun updateRunStatus(
        runId: String,
        status: RunStatusValue,
        exitCode: Int? = null,
        pid: Long? = null,
        endedAtEpochMs: Long? = null,
        pidStartedAtEpochMs: Long? = null
    ) {
        val run = runs[runId] ?: return
        // Rule: ExecutionFinished / terminal updates never overwrite CANCELLED
        if (run.status == RunStatusValue.CANCELLED && (status == RunStatusValue.COMPLETED || status == RunStatusValue.FAILED)) {
            return
        }
        run.status = status
        if (exitCode != null) run.exitCode = exitCode
        if (pid != null) run.pid = pid
        if (endedAtEpochMs != null) run.endedAtEpochMs = endedAtEpochMs

        // The in-memory status is already final. A failed write must not stop the caller from delivering the
        // run's ExecutionFinished event, so it is reported and the next rehydrate sees the older record.
        try {
            journalStore.updateRunMetadata(runId) { current ->
                current.copy(
                    status = run.status,
                    exitCode = run.exitCode ?: current.exitCode,
                    pid = run.pid ?: current.pid,
                    pidStartedAtEpochMs = pidStartedAtEpochMs ?: current.pidStartedAtEpochMs,
                    endedAtEpochMs = run.endedAtEpochMs ?: current.endedAtEpochMs
                )
            }
        } catch (e: java.io.IOException) {
            System.err.println("ExecutionSessionManager: could not persist status $status of run $runId: ${e.message}")
        }
    }

    /**
     * The run, or - if it was dropped from memory for age - its finished record from the journal, so a client
     * that comes back for the output of an old build still gets it. Only finished runs are restored: one the
     * journal still shows as active but memory does not know belongs to a dead service.
     */
    public fun getRun(runId: String): ServerRun? = runs[runId] ?: runId.takeIf { SAFE_RUN_ID.matches(it) }
        ?.let(journalStore::loadRun)
        ?.takeIf { it.status in FINISHED }
        ?.let(::restoredRun)

    public fun listRuns(workspaceLock: String? = null): List<RunStatus> {
        return runs.values
            .filter { workspaceLock == null || it.workspaceLock == workspaceLock }
            .map { it.toStatus() }
    }
}

public class ServerRun(
    public val runId: String,
    public val request: StartRunRequest,
    public val idempotencyKey: String,
    public val workspaceLock: String?,
    @Volatile public var status: RunStatusValue = RunStatusValue.QUEUED,
    public val startedAtEpochMs: Long = System.currentTimeMillis(),
    @Volatile public var exitCode: Int? = null,
    @Volatile public var pid: Long? = null,
    @Volatile public var endedAtEpochMs: Long? = null,
    public val session: ExecutionSession,
    private val journalStore: RunJournalStore? = null
) {
    public fun toStatus(): RunStatus = RunStatus(
        runId = runId,
        status = status,
        exitCode = exitCode,
        pid = pid,
        startedAtEpochMs = startedAtEpochMs,
        endedAtEpochMs = endedAtEpochMs,
        lastSeq = session.lastSeq() ?: (journalStore?.loadRun(runId)?.lastSeq ?: 0L)
    )
}

/** A bounded piece of an event history, and where in the journal file the next piece starts. */
public class ReplayPage(
    public val events: List<SequencedStreamEvent>,
    public val journalOffset: Long,
    /** Set when the history before this sequence number is lost: the page continues from it. */
    public val skippedToSeq: Long? = null
)

private const val REPLAY_PAGE_EVENTS = 500
