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
    private val processRunner: ProcessExecutionPort? = null,
    private val toolchainStateMachine: io.ltirom.server.toolchain.ToolchainStateMachine? = null
) : LtiRomServerEngine {

    private val serverScope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.SupervisorJob() + kotlinx.coroutines.Dispatchers.IO)

    public constructor(
        toolResolver: ToolResolverPort,
        processRunner: ProcessExecutionPort,
        distroName: String,
        startTimeEpochMs: Long = System.currentTimeMillis(),
        sessionManager: ExecutionSessionManager = ExecutionSessionManager(),
        toolchainStateMachine: io.ltirom.server.toolchain.ToolchainStateMachine? = null
    ) : this(
        executeToolUseCase = ExecuteToolUseCase(toolResolver, processRunner),
        streamToolUseCase = StreamToolUseCase(toolResolver, processRunner),
        cancelProcessUseCase = (processRunner as? ProcessCancellationPort)?.let { CancelProcessUseCase(it) },
        translatePathUseCase = TranslatePathUseCase(),
        healthUseCase = GetServerHealthUseCase(toolResolver, distroName, startTimeEpochMs),
        sessionManager = sessionManager,
        processRunner = processRunner,
        toolchainStateMachine = toolchainStateMachine
    )

    override suspend fun execute(request: ToolExecutionRequest, traceId: String?): ToolExecutionResponse =
        admitted("execute:${traceId ?: request.toolId}") { executeToolUseCase.execute(request) }

    override fun stream(
        request: ToolExecutionRequest,
        traceId: String?,
        sessionId: String?
    ): Flow<SequencedStreamEvent> = flow {
        val sId = sessionId ?: (traceId ?: "sess-${UUID.randomUUID().toString().take(8)}")
        admitted("stream:$sId") {
            val session = sessionManager.createSession(
                toolId = request.toolId,
                arguments = request.arguments,
                sessionId = sId
            )

            streamToolUseCase.stream(request) { rawEvent ->
                val sequenced = session.appendEvent(rawEvent)
                emit(sequenced)
            }
        }
    }

    override fun cancel(executionId: String, signal: String): Boolean {
        sessionManager.getSession(executionId)?.markCancelled()
        return cancelProcessUseCase?.cancel(executionId, signal) ?: false
    }

    override fun getHealth(): WslServerInfo {
        val base = healthUseCase.getHealth()
        return base.copy(
            activeRuns = activeWork().size,
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
        val sm = toolchainStateMachine
        if (sm != null && !sm.canAdmit(request.purpose)) {
            val err = if (sm.state == io.ltirom.server.toolchain.ToolchainState.UNINITIALIZED) "TOOLCHAIN_UNINITIALIZED" else "TOOLCHAIN_RECOVERY"
            return RunStartOutcome.ToolchainNotReady(sm.state, err)
        }

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
        // One atomic section: two concurrent requests can't both miss each other's duplicate key or workspace
        // lock, and a shutdown that saw no active work can never be followed by a run it would then kill.
        // ponytail: one engine-wide lock; per-workspace locks if run admission ever becomes a bottleneck.
        val run = synchronized(admissionLock) {
            sessionManager.findIdempotentRun(request.idempotencyKey)?.let { return RunStartOutcome.Existing(it) }
            if (!request.workspaceLock.isNullOrBlank()) {
                sessionManager.findConflictingRun(request.workspaceLock)?.let {
                    return RunStartOutcome.Conflict(RunConflict(existingRunId = it.runId, message = "Workspace is busy"))
                }
            }
            if (holds.isNotEmpty()) return RunStartOutcome.ShuttingDown
            sessionManager.registerRun(
                runId = runId,
                request = request,
                idempotencyKey = request.idempotencyKey,
                workspaceLock = request.workspaceLock
            )
        }

        val runner = processRunner
        if (runner != null) {
            serverScope.launch {
                run.status = RunStatusValue.RUNNING
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
                            is ProcessOutputChunk.Stdout -> run.session.appendEvent(StreamEvent.OutputChunk(chunk.line, isError = false))
                            is ProcessOutputChunk.Stderr -> run.session.appendEvent(StreamEvent.OutputChunk(chunk.line, isError = true))
                            is ProcessOutputChunk.Exit -> {
                                val finalStatus = if (run.status == RunStatusValue.CANCELLING || run.status == RunStatusValue.CANCELLED) {
                                    RunStatusValue.CANCELLED
                                } else if (chunk.exitCode == 0) {
                                    RunStatusValue.COMPLETED
                                } else {
                                    RunStatusValue.FAILED
                                }
                                run.session.appendEvent(StreamEvent.ExecutionFinished(chunk.exitCode, chunk.durationMs, ""))
                                sessionManager.updateRunStatus(
                                    runId = runId,
                                    status = finalStatus,
                                    exitCode = chunk.exitCode,
                                    endedAtEpochMs = System.currentTimeMillis()
                                )
                            }
                        }
                    }
                } catch (e: Exception) {
                    val finalStatus = if (run.status == RunStatusValue.CANCELLING || run.status == RunStatusValue.CANCELLED) {
                        RunStatusValue.CANCELLED
                    } else {
                        RunStatusValue.FAILED
                    }
                    run.session.appendEvent(StreamEvent.ExecutionFinished(-1, 0L, e.message ?: "Execution error"))
                    sessionManager.updateRunStatus(
                        runId = runId,
                        status = finalStatus,
                        exitCode = -1,
                        endedAtEpochMs = System.currentTimeMillis()
                    )
                }
            }
        }

        val handle = RunHandle(
            runId = runId,
            status = RunStatusValue.QUEUED,
            startedAtEpochMs = run.startedAtEpochMs
        )
        return RunStartOutcome.Created(handle)
    }

    override fun getRun(runId: String): RunStatus? = sessionManager.getRun(runId)?.toStatus()

    override fun listRuns(workspaceLock: String?): List<RunStatus> = sessionManager.listRuns(workspaceLock)

    override fun attachRun(runId: String, fromSeq: Long): Flow<SequencedStreamEvent>? {
        val run = sessionManager.getRun(runId) ?: return null
        // A finished run (possibly reloaded from the journal after a restart) has no live producer: replay
        // what is stored and end, instead of waiting for live events that will never come.
        if (run.status in TERMINAL_RUN_STATUSES) {
            return flow { run.session.replay(fromSeq).forEach { emit(it) } }
        }
        return run.session.attach(fromSeq)
    }

    private companion object {
        val TERMINAL_RUN_STATUSES = setOf(
            RunStatusValue.COMPLETED,
            RunStatusValue.FAILED,
            RunStatusValue.CANCELLED,
            RunStatusValue.INTERRUPTED,
        )
    }

    override fun cancelRun(runId: String, signal: String): RunCancelResponse {
        val run = sessionManager.getRun(runId)
            ?: return RunCancelResponse(accepted = false, status = RunStatusValue.FAILED, message = "Run not found")

        val terminalStatuses = setOf(
            RunStatusValue.COMPLETED,
            RunStatusValue.FAILED,
            RunStatusValue.CANCELLED,
            RunStatusValue.INTERRUPTED
        )
        if (run.status in terminalStatuses) {
            return RunCancelResponse(accepted = false, status = run.status, message = "Run is already terminal")
        }

        sessionManager.updateRunStatus(runId, RunStatusValue.CANCELLING)
        run.session.markCancelled()
        val runner = processRunner as? ProcessCancellationPort
        val cancelled = runner?.cancelProcess(runId, signal) ?: false
        return RunCancelResponse(
            accepted = true,
            status = RunStatusValue.CANCELLING,
            message = if (cancelled) "Signal $signal sent to process" else "Cancellation initiated"
        )
    }

    override fun activeRuns(): List<ServerRun> = sessionManager.activeRuns()

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
        val active = activeWork()
        if (active.isEmpty()) {
            holds.add(hold)
            emptyList()
        } else {
            active
        }
    }

    /** Legacy `execute`/`stream` calls in flight (they are not runs); guarded by [admissionLock]. */
    private val legacyWork = mutableListOf<String>()

    private fun activeWork(): List<String> = synchronized(admissionLock) {
        sessionManager.activeRuns().map { it.runId } + legacyWork
    }

    /** Runs a legacy execution under the same admission gate and accounting as runs. */
    private inline fun <T> admitted(label: String, block: () -> T): T {
        synchronized(admissionLock) {
            if (holds.isNotEmpty()) throw ServiceShuttingDownException()
            legacyWork.add(label)
        }
        try {
            return block()
        } finally {
            synchronized(admissionLock) { legacyWork.remove(label) }
        }
    }

    override fun closeAdmission(force: Boolean): List<String> = synchronized(admissionLock) {
        val active = activeWork()
        if (force || active.isEmpty()) holds.add(Hold.Shutdown)
        if (force) emptyList() else active
    }
}

