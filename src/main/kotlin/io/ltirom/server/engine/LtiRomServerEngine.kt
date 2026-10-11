package io.ltirom.server.engine

import io.ltirom.server.application.*
import io.ltirom.server.domain.ports.ProcessCancellationPort
import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.model.*
import io.ltirom.server.session.ExecutionSession
import io.ltirom.server.session.ExecutionSessionManager
import io.ltirom.server.session.ServerRun
import io.ltirom.server.domain.ports.ProcessOutputChunk
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.util.UUID

/**
 * Framework-independent core server application contract.
 * Decouples all business logic, process supervision, and session management from
 * delivery protocols (Ktor, WebSockets, UNIX Domain Sockets, Stdio CLI).
 */
public interface LtiRomServerEngine {
    public suspend fun execute(request: ToolExecutionRequest, traceId: String? = null): ToolExecutionResponse
    public fun stream(request: ToolExecutionRequest, traceId: String? = null, sessionId: String? = null): Flow<SequencedStreamEvent>
    public fun cancel(executionId: String, signal: String = "SIGTERM"): Boolean
    public fun getHealth(): WslServerInfo
    public fun listTools(): List<ToolStatusInfo>
    public fun refreshTools(): List<ToolStatusInfo>
    public suspend fun translatePath(request: PathTranslationRequest): PathTranslationResponse
    public fun listSessions(): List<SessionSummary>
    public fun getSession(sessionId: String): SessionSummary?
    public fun replaySession(sessionId: String, fromSeq: Long = 0L): List<SequencedStreamEvent>
    public suspend fun startRun(request: StartRunRequest): RunStartOutcome
    public fun getRun(runId: String): RunStatus?
    public fun listRuns(workspaceLock: String? = null): List<RunStatus>
    public fun attachRun(runId: String, fromSeq: Long = 0L): Flow<SequencedStreamEvent>?
    public fun cancelRun(runId: String, signal: String = "SIGTERM"): RunCancelResponse
    public fun activeRuns(): List<ServerRun>

    /** Durable runs and direct executions currently admitted by the engine. */
    public fun activeWork(): List<String>

    public val admissionHolds: Set<Hold>
    public fun addHold(hold: Hold)
    public fun removeHold(hold: Hold)
    public fun tryAddHold(hold: Hold): List<String>

    /**
     * Stops admitting new work if none is active, atomically with the check; returns the IDs of the work
     * still active (runs and legacy executions; empty means admission is now closed). With [force],
     * admission closes regardless.
     */
    public fun closeAdmission(force: Boolean = false): List<String>

    /** Reopens generic client admission after a new authenticated heartbeat. */
    public fun reopenAdmission()
}

/** Thrown by the legacy [LtiRomServerEngine.execute]/[LtiRomServerEngine.stream] once admission is closed. */
public class ServiceShuttingDownException : IllegalStateException("The build service is shutting down")

/**
 * Default domain implementation orchestrating hexagonal use cases and session management.
 */
public class DefaultServerEngine(
    private val executeToolUseCase: ExecuteToolUseCase,
    private val streamToolUseCase: StreamToolUseCase,
    private val cancelProcessUseCase: CancelProcessUseCase? = null,
    private val translatePathUseCase: TranslatePathUseCase = TranslatePathUseCase(),
    private val healthUseCase: GetServerHealthUseCase,
    public val sessionManager: ExecutionSessionManager = ExecutionSessionManager(),
    private val processRunner: ProcessExecutionPort? = null
) : LtiRomServerEngine {

    private val serverScope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.SupervisorJob() + kotlinx.coroutines.Dispatchers.IO)
    private val runJobs = java.util.concurrent.ConcurrentHashMap<String, kotlinx.coroutines.Job>()

    /** Processes of runs a previous service started and that were still alive when this one came up. */
    private val adopted = java.util.concurrent.ConcurrentHashMap<String, AdoptedProcess>()

    /**
     * A surviving run's process and every descendant seen so far. Handles (not pids) are kept, so a pid reused by
     * an unrelated process is never mistaken for one of these.
     */
    private class AdoptedProcess(val root: ProcessHandle) {
        private val tree = java.util.concurrent.ConcurrentHashMap.newKeySet<ProcessHandle>()

        fun refresh() {
            runCatching { root.descendants().use { it.forEach(tree::add) } }
            tree.toList().forEach { handle ->
                if (handle.isAlive) runCatching { handle.descendants().use { it.forEach(tree::add) } }
            }
        }

        fun alive(): List<ProcessHandle> = (tree + root).filter { it.isAlive }
    }

    public constructor(
        toolResolver: ToolResolverPort,
        processRunner: ProcessExecutionPort,
        distroName: String,
        startTimeEpochMs: Long = System.currentTimeMillis(),
        sessionManager: ExecutionSessionManager = ExecutionSessionManager()
    ) : this(
        executeToolUseCase = ExecuteToolUseCase(toolResolver, processRunner),
        streamToolUseCase = StreamToolUseCase(toolResolver, processRunner),
        cancelProcessUseCase = (processRunner as? ProcessCancellationPort)?.let { CancelProcessUseCase(it) },
        translatePathUseCase = TranslatePathUseCase(),
        healthUseCase = GetServerHealthUseCase(toolResolver, distroName, startTimeEpochMs),
        sessionManager = sessionManager,
        processRunner = processRunner
    )

    private val directExecutionIds = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()

    /**
     * A run the journal still shows as active, whose process (same pid, same start time) is alive, belongs to a
     * previous service. It stays active work - so an update waits for it and admission accounts for it - and is
     * watched until the process ends. Nothing is lost track of, and nothing is reported as dead while it runs.
     */
    private fun adoptSurvivingRuns() {
        for (run in sessionManager.activeRuns()) {
            val handle = run.pid?.let { ProcessHandle.of(it).orElse(null) } ?: continue
            val process = AdoptedProcess(handle)
            adopted[run.runId] = process
            serverScope.launch {
                // Over only when the process *and everything it started* is gone: a parent that exits promptly
                // can leave a child behind, and that child still belongs to this run.
                while (true) {
                    process.refresh()
                    if (process.alive().isEmpty()) break
                    kotlinx.coroutines.delay(ADOPTED_POLL_MS)
                }
                adopted.remove(run.runId)
                val cancelled = run.status == RunStatusValue.CANCELLING
                settleTerminalRun(
                    run.runId,
                    if (cancelled) RunStatusValue.CANCELLED else RunStatusValue.INTERRUPTED,
                    exitCode = if (cancelled) 130 else -1,
                    summary = if (cancelled) {
                        "Cancelled"
                    } else {
                        "The service restarted while this run was active; it ended afterwards and its result was not seen"
                    },
                )
            }
        }
    }

    /** Asks the whole tree to stop, then forces whatever is still alive after the grace period. */
    private fun stopAdopted(process: AdoptedProcess) {
        process.refresh()
        process.alive().forEach { it.destroy() }
        serverScope.launch {
            kotlinx.coroutines.delay(ADOPTED_GRACE_SECONDS * MS_PER_SECOND)
            process.refresh()
            process.alive().forEach { it.destroyForcibly() }
        }
    }

    override suspend fun execute(request: ToolExecutionRequest, traceId: String?): ToolExecutionResponse {
        val execId = requireSafeExecutionId(traceId ?: "exec-${UUID.randomUUID().toString().take(8)}")
        return admitted("execute", execId) {
            directExecutionIds.add(execId)
            executeToolUseCase.execute(request, executionId = execId)
        }
    }

    override fun stream(
        request: ToolExecutionRequest,
        traceId: String?,
        sessionId: String?
    ): Flow<SequencedStreamEvent> = flow {
        val sId = requireSafeExecutionId(sessionId ?: (traceId ?: "sess-${UUID.randomUUID().toString().take(8)}"))
        admitted("stream", sId) {
            directExecutionIds.add(sId)
            val session = sessionManager.createSession(
                toolId = request.toolId,
                arguments = request.arguments,
                sessionId = sId
            )

            streamToolUseCase.stream(request, executionId = sId) { rawEvent ->
                val sequenced = session.appendEvent(rawEvent)
                emit(sequenced)
            }
        }
    }

    override fun cancel(executionId: String, signal: String): Boolean {
        val terminal = cancelProcessUseCase?.cancel(executionId, signal) ?: false
        if (terminal) {
            sessionManager.getSession(executionId)?.markCancelled()
            directExecutionIds.remove(executionId)
        }
        return terminal
    }

    override fun getHealth(): WslServerInfo {
        val base = healthUseCase.getHealth()
        val work = activeWork()
        return base.copy(
            activeRuns = work.size,
            activeWork = work,
            protocolVersion = "1.2.0"
        )
    }

    override fun listTools(): List<ToolStatusInfo> = healthUseCase.listTools()

    override fun refreshTools(): List<ToolStatusInfo> = healthUseCase.refreshTools()

    override suspend fun translatePath(request: PathTranslationRequest): PathTranslationResponse =
        translatePathUseCase.translate(request)

    override fun listSessions(): List<SessionSummary> = sessionManager.listSessions()

    override fun getSession(sessionId: String): SessionSummary? = sessionManager.getSession(sessionId)?.toSummary()

    override fun replaySession(sessionId: String, fromSeq: Long): List<SequencedStreamEvent> =
        sessionManager.getSession(sessionId)?.ringBuffer?.replay(fromSeq) ?: emptyList()

    override suspend fun startRun(request: StartRunRequest): RunStartOutcome {
        val cwdStr = request.request.workingDirectory
        if (cwdStr.isNullOrBlank()) {
            return RunStartOutcome.InvalidCwd(InvalidWorkingDirectory(path = cwdStr, reason = "working directory missing"))
        }
        val cwdFile = java.io.File(cwdStr)
        if (!cwdFile.isAbsolute) {
            return RunStartOutcome.InvalidCwd(InvalidWorkingDirectory(path = cwdStr, reason = "working directory must be absolute"))
        }
        if (!cwdFile.isDirectory) {
            return RunStartOutcome.InvalidCwd(InvalidWorkingDirectory(path = cwdStr, reason = "working directory does not exist or is not a directory"))
        }

        val runId = UUID.randomUUID().toString()
        var job: kotlinx.coroutines.Job? = null
        val run = synchronized(admissionLock) {
            sessionManager.findIdempotentRun(request.idempotencyKey)?.let { return RunStartOutcome.Existing(it) }
            if (!request.workspaceLock.isNullOrBlank()) {
                sessionManager.findConflictingRun(request.workspaceLock)?.let {
                    return RunStartOutcome.Conflict(RunConflict(existingRunId = it.runId, message = "Workspace is busy"))
                }
            }
            if (holds.isNotEmpty()) return RunStartOutcome.ShuttingDown
            val regRun = sessionManager.registerRun(
                runId = runId,
                request = request,
                idempotencyKey = request.idempotencyKey,
                workspaceLock = request.workspaceLock
            )
            val runner = processRunner
            if (runner != null) {
                val lazyJob = serverScope.launch(start = kotlinx.coroutines.CoroutineStart.LAZY) {
                    val canStart = synchronized(admissionLock) {
                        if (regRun.status == RunStatusValue.CANCELLING || regRun.status == RunStatusValue.CANCELLED) {
                            false
                        } else {
                            regRun.status = RunStatusValue.RUNNING
                            true
                        }
                    }
                    if (!canStart) {
                        settleTerminalRun(runId, RunStatusValue.CANCELLED, exitCode = 130, summary = "Cancelled before start")
                        return@launch
                    }
                    try {
                        runner.runProcessFlow(
                            toolName = request.request.toolId,
                            arguments = request.request.arguments,
                            workingDir = cwdFile,
                            environment = request.request.environment,
                            stdinText = request.request.stdinText,
                            timeoutMs = request.request.timeoutMs,
                            executionId = runId
                        ).collect { chunk ->
                            when (chunk) {
                                is ProcessOutputChunk.Started ->
                                    // The run is RUNNING and has an identity from now on, on disk too: a restarted
                                    // service finds this process again instead of declaring the run dead.
                                    recordIdentityOrStop(runId, chunk.pid, chunk.startedAtEpochMs)
                                is ProcessOutputChunk.Stdout -> regRun.session.appendEvent(StreamEvent.OutputChunk(chunk.line, isError = false))
                                is ProcessOutputChunk.Stderr -> regRun.session.appendEvent(StreamEvent.OutputChunk(chunk.line, isError = true))
                                is ProcessOutputChunk.Exit -> {
                                    val finalStatus = synchronized(admissionLock) {
                                        if (regRun.status == RunStatusValue.CANCELLING || regRun.status == RunStatusValue.CANCELLED) {
                                            RunStatusValue.CANCELLED
                                        } else if (chunk.exitCode == 0) {
                                            RunStatusValue.COMPLETED
                                        } else {
                                            RunStatusValue.FAILED
                                        }
                                    }
                                    settleTerminalRun(runId, finalStatus, chunk.exitCode, "")
                                }
                            }
                        }
                    } catch (ce: CancellationException) {
                        settleTerminalRun(runId, RunStatusValue.CANCELLED, exitCode = 130, summary = "Cancelled")
                    } catch (e: Exception) {
                        settleTerminalRun(runId, RunStatusValue.FAILED, exitCode = -1, summary = e.message ?: "Execution error")
                    }
                }
                lazyJob.invokeOnCompletion { cause ->
                    if (cause != null) {
                        settleTerminalRun(runId, RunStatusValue.CANCELLED, exitCode = 130, summary = "Cancelled")
                    }
                }
                runJobs[runId] = lazyJob
                job = lazyJob
            }
            regRun
        }

        job?.start()

        val handle = RunHandle(
            runId = runId,
            status = RunStatusValue.QUEUED,
            startedAtEpochMs = run.startedAtEpochMs
        )
        return RunStartOutcome.Created(handle)
    }

    /**
     * A process that no record names is out of reach of a restarted service. If the record cannot be written the
     * process is stopped and the run fails, saying whether the stop was confirmed.
     */
    private suspend fun recordIdentityOrStop(runId: String, pid: Long, startedAtEpochMs: Long?) {
        val failure = try {
            sessionManager.recordProcessIdentity(runId, pid, startedAtEpochMs)
            return
        } catch (e: java.io.IOException) {
            e
        }
        val port = processRunner as? ProcessCancellationPort
        port?.cancelProcess(runId, "SIGKILL")
        val stopped = port != null && kotlinx.coroutines.withTimeoutOrNull(IDENTITY_STOP_WAIT_MS) {
            while (port.getSurvivors(runId).isNotEmpty()) delay(IDENTITY_STOP_POLL_MS)
            true
        } == true
        val outcome = if (stopped) "the process was stopped" else "the process (pid $pid) could not be confirmed stopped"
        throw IllegalStateException("Could not record the process identity (${failure.message}); $outcome")
    }

    private fun settleTerminalRun(
        runId: String,
        finalStatus: RunStatusValue,
        exitCode: Int,
        summary: String = ""
    ): Boolean = synchronized(admissionLock) {
        val run = sessionManager.getRun(runId) ?: return false
        if (run.status in TERMINAL_RUN_STATUSES) return false
        val durationMs = (System.currentTimeMillis() - run.startedAtEpochMs).coerceAtLeast(0L)
        sessionManager.updateRunStatus(
            runId = runId,
            status = finalStatus,
            exitCode = exitCode,
            endedAtEpochMs = System.currentTimeMillis()
        )
        run.session.appendEvent(StreamEvent.ExecutionFinished(exitCode, durationMs, summary))
        runJobs.remove(runId)
        true
    }

    override fun getRun(runId: String): RunStatus? {
        val run = sessionManager.getRun(runId) ?: return null
        val survivors = (processRunner as? ProcessCancellationPort)?.getSurvivors(runId).orEmpty()
        val baseStatus = run.toStatus()
        return if (survivors.isNotEmpty()) {
            baseStatus.copy(unconfirmedSurvivors = survivors)
        } else {
            baseStatus
        }
    }

    override fun listRuns(workspaceLock: String?): List<RunStatus> = sessionManager.listRuns(workspaceLock)

    override fun attachRun(runId: String, fromSeq: Long): Flow<SequencedStreamEvent>? {
        val run = sessionManager.getRun(runId) ?: return null
        if (run.status in TERMINAL_RUN_STATUSES) {
            return run.session.replayFlow(fromSeq)
        }
        return run.session.attach(fromSeq)
    }

    private companion object {
        const val IDENTITY_STOP_WAIT_MS = 5_000L
        const val IDENTITY_STOP_POLL_MS = 50L
        const val ADOPTED_GRACE_SECONDS = 5L
        const val ADOPTED_POLL_MS = 200L
        const val MS_PER_SECOND = 1_000L
        val TERMINAL_RUN_STATUSES = setOf(
            RunStatusValue.COMPLETED,
            RunStatusValue.FAILED,
            RunStatusValue.CANCELLED,
            RunStatusValue.INTERRUPTED,
        )
    }

    override fun cancelRun(runId: String, signal: String): RunCancelResponse {
        val jobToCancel = synchronized(admissionLock) {
            val run = sessionManager.getRun(runId)
                ?: return RunCancelResponse(accepted = false, status = RunStatusValue.FAILED, message = "Run not found")

            val hasSurvivors = (processRunner as? ProcessCancellationPort)?.getSurvivors(runId)?.isNotEmpty() == true
            if (run.status in TERMINAL_RUN_STATUSES && !hasSurvivors) {
                return RunCancelResponse(accepted = false, status = run.status, message = "Run is already terminal")
            }

            if (run.status == RunStatusValue.QUEUED) {
                runJobs.remove(runId)?.cancel()
                settleTerminalRun(runId, RunStatusValue.CANCELLED, exitCode = 130, summary = "Cancelled while queued")
                return RunCancelResponse(accepted = true, status = RunStatusValue.CANCELLED, message = "Cancelled while queued")
            }

            run.status = RunStatusValue.CANCELLING
            run.session.markCancelled()
            runJobs[runId]
        }

        val runner = processRunner as? ProcessCancellationPort
        val cancelled = runner?.cancelProcess(runId, signal) ?: false
        // A run of a previous service has no worker here; its process is stopped directly and settles on exit.
        adopted[runId]?.let(::stopAdopted)
        jobToCancel?.cancel()
        if (cancelled) {
            settleTerminalRun(runId, RunStatusValue.CANCELLED, exitCode = 130, summary = "Cancelled by signal $signal")
        }
        return RunCancelResponse(
            accepted = true,
            status = RunStatusValue.CANCELLING,
            message = if (cancelled) "Signal $signal sent to process and worker cancelled" else "Cancellation initiated"
        )
    }

    override fun activeRuns(): List<ServerRun> = sessionManager.activeRuns()

    override fun activeWork(): List<String> = activeWorkInternal()

    private val admissionLock = Any()

    private val holds = mutableSetOf<Hold>()

    override val admissionHolds: Set<Hold>
        get() = synchronized(admissionLock) { holds.toSet() }

    override fun addHold(hold: Hold) {
        synchronized(admissionLock) { holds.add(hold) }
    }

    override fun removeHold(hold: Hold) {
        synchronized(admissionLock) { holds.remove(hold) }
    }

    override fun tryAddHold(hold: Hold): List<String> = synchronized(admissionLock) {
        val active = activeWorkInternal()
        if (active.isEmpty()) {
            holds.add(hold)
            emptyList()
        } else {
            active
        }
    }

    /** Legacy `execute`/`stream` calls in flight (they are not runs); guarded by [admissionLock]. */
    private val legacyWork = mutableListOf<String>()

    // Last of the properties: adoption may settle a run at once, and that uses the locks above.
    init {
        adoptSurvivingRuns()
    }

    private fun activeWorkInternal(): List<String> = synchronized(admissionLock) {
        val cancellationPort = processRunner as? ProcessCancellationPort
        val activeRunIds = sessionManager.activeRuns().map { it.runId }
        val runsWithSurvivors = sessionManager.listRuns().filter { run ->
            run.status == RunStatusValue.CANCELLING || cancellationPort?.getSurvivors(run.runId)?.isNotEmpty() == true
        }.map { it.runId }
        val directWithSurvivors = directExecutionIds.filter { id ->
            cancellationPort?.getSurvivors(id)?.isNotEmpty() == true
        }.map { "execute:$it" }
        directExecutionIds.removeIf { id ->
            cancellationPort?.getSurvivors(id)?.isEmpty() == true && "execute:$id" !in legacyWork && "stream:$id" !in legacyWork
        }
        (activeRunIds + runsWithSurvivors + legacyWork + directWithSurvivors).distinct()
    }

    /** Runs a legacy execution under the same admission gate and accounting as runs. */
    private inline fun <T> admitted(kind: String, id: String, block: () -> T): T {
        val label = "$kind:$id"
        synchronized(admissionLock) {
            if (holds.isNotEmpty()) throw ServiceShuttingDownException()
            // Checked and taken in one step: a second request with the same id would overwrite the first one's
            // process entry, and the first to finish would then remove the tracking of the other.
            if (legacyWork.any { it.substringAfter(':') == id } || sessionManager.activeRuns().any { it.runId == id }) {
                throw DuplicateExecutionException(id)
            }
            legacyWork.add(label)
        }
        try {
            return block()
        } finally {
            synchronized(admissionLock) { legacyWork.remove(label) }
        }
    }

    override fun closeAdmission(force: Boolean): List<String> = synchronized(admissionLock) {
        val active = activeWorkInternal()
        if (force || active.isEmpty()) holds.add(Hold.ClientLifecycle)
        active
    }

    override fun reopenAdmission() {
        synchronized(admissionLock) { holds.remove(Hold.ClientLifecycle) }
    }
}
