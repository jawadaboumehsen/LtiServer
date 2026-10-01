package io.ltirom.server.session

import io.ltirom.server.infra.RunJournalStore
import io.ltirom.server.model.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.asSharedFlow
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicLong

/**
 * Thread-safe circular ring buffer retaining the last [capacity] sequenced events.
 */
public class CircularEventBuffer(
    public val capacity: Int = 10_000,
    initialSeq: Long = 0L
) {
    private val deque = ConcurrentLinkedDeque<SequencedStreamEvent>()
    private val nextSeq = AtomicLong(initialSeq)

    public fun append(event: StreamEvent): SequencedStreamEvent {
        val seq = nextSeq.getAndIncrement()
        val sequenced = SequencedStreamEvent(seq, event)
        deque.add(sequenced)
        while (deque.size > capacity) {
            deque.pollFirst()
        }
        return sequenced
    }

    public fun replay(fromSeq: Long = 0L): List<SequencedStreamEvent> {
        return deque.filter { it.seq >= fromSeq }
    }

    public fun floorSeq(): Long? = deque.peekFirst()?.seq

    public fun lastSeq(): Long? = deque.peekLast()?.seq
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
    private val _status = MutableStateFlow(SessionStatus.RUNNING)
    public val status = _status.asStateFlow()

    public var exitCode: Int? = null
        private set
    public var durationMs: Long? = null
        private set

    private val _liveEvents = kotlinx.coroutines.flow.MutableSharedFlow<SequencedStreamEvent>(extraBufferCapacity = 1000)
    public val liveEvents: kotlinx.coroutines.flow.SharedFlow<SequencedStreamEvent> = _liveEvents.asSharedFlow()

    public fun appendEvent(event: StreamEvent): SequencedStreamEvent {
        if (event is StreamEvent.ExecutionFinished) {
            exitCode = event.exitCode
            durationMs = event.durationMs
            if (_status.value != SessionStatus.CANCELLED) {
                _status.value = if (event.exitCode == 0) SessionStatus.COMPLETED else SessionStatus.FAILED
            }
        }
        val sequenced = ringBuffer.append(event)
        journalStore?.appendEvent(sessionId, sequenced)
        _liveEvents.tryEmit(sequenced)
        return sequenced
    }

    public fun markCancelled() {
        _status.value = SessionStatus.CANCELLED
    }

    public fun replay(fromSeq: Long = 0L): List<SequencedStreamEvent> {
        val floor = ringBuffer.floorSeq()
        if (journalStore != null && (floor == null || fromSeq < floor)) {
            return journalStore.replay(sessionId, fromSeq)
        }
        return ringBuffer.replay(fromSeq)
    }

    public fun attach(fromSeq: Long = 0L): kotlinx.coroutines.flow.Flow<SequencedStreamEvent> = kotlinx.coroutines.flow.flow {
        val replayed = replay(fromSeq)
        var highestSeq = fromSeq - 1L
        for (event in replayed) {
            if (event.seq > highestSeq) highestSeq = event.seq
            emit(event)
        }
        if (replayed.any { it.event is StreamEvent.ExecutionFinished }) {
            return@flow
        }
        try {
            liveEvents.collect { liveEvent ->
                if (liveEvent.seq > highestSeq) {
                    highestSeq = liveEvent.seq
                    emit(liveEvent)
                }
                if (liveEvent.event is StreamEvent.ExecutionFinished) {
                    throw kotlinx.coroutines.CancellationException("Finished")
                }
            }
        } catch (_: kotlinx.coroutines.CancellationException) {
            // Clean exit
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
            val session = ExecutionSession(
                sessionId = meta.runId,
                toolId = meta.request.request.toolId,
                arguments = meta.request.request.arguments,
                bufferCapacity = defaultCapacity,
                journalStore = journalStore,
                initialSeq = meta.lastSeq + 1L
            )
            val run = ServerRun(
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
            runs[meta.runId] = run
            val handle = RunHandle(runId = meta.runId, status = meta.status, startedAtEpochMs = meta.startedAtEpochMs)
            idempotencyMap[meta.idempotencyKey] = Pair(handle, meta.startedAtEpochMs)
        }
        return persisted
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

    public fun registerRun(
        runId: String,
        request: StartRunRequest,
        idempotencyKey: String,
        workspaceLock: String?
    ): ServerRun {
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
            startedAtEpochMs = System.currentTimeMillis(),
            session = session,
            journalStore = journalStore
        )
        runs[runId] = run
        val handle = RunHandle(runId = runId, status = RunStatusValue.QUEUED, startedAtEpochMs = run.startedAtEpochMs)
        idempotencyMap[idempotencyKey] = Pair(handle, run.startedAtEpochMs)

        val metadata = PersistedRunMetadata(
            runId = runId,
            request = request,
            workingDirectory = request.request.workingDirectory ?: "",
            workspaceLock = workspaceLock,
            idempotencyKey = idempotencyKey,
            status = RunStatusValue.QUEUED,
            startedAtEpochMs = run.startedAtEpochMs,
            lastSeq = 0L
        )
        journalStore.recordRun(metadata)

        return run
    }

    public fun updateRunStatus(
        runId: String,
        status: RunStatusValue,
        exitCode: Int? = null,
        pid: Long? = null,
        endedAtEpochMs: Long? = null
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

        journalStore.updateRunMetadata(runId) { current ->
            current.copy(
                status = run.status,
                exitCode = run.exitCode ?: current.exitCode,
                pid = run.pid ?: current.pid,
                endedAtEpochMs = run.endedAtEpochMs ?: current.endedAtEpochMs
            )
        }
    }

    public fun getRun(runId: String): ServerRun? = runs[runId]

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
