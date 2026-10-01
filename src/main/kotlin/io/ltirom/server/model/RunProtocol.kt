package io.ltirom.server.model

import kotlinx.serialization.Serializable

@Serializable
public enum class RunStatusValue {
    QUEUED,
    RUNNING,
    CANCELLING,
    CANCELLED,
    COMPLETED,
    FAILED,
    INTERRUPTED
}

@Serializable
public enum class RunPurpose {
    SETUP,
    WORKSPACE
}

@Serializable
public data class StartRunRequest(
    val request: ToolExecutionRequest,
    val idempotencyKey: String,
    val workspaceLock: String? = null,
    val purpose: RunPurpose = RunPurpose.WORKSPACE
)

@Serializable
public data class RunHandle(
    val runId: String,
    val status: RunStatusValue,
    val startedAtEpochMs: Long
)

@Serializable
public data class RunConflict(
    val existingRunId: String,
    val message: String? = null
)

@Serializable
public data class InvalidWorkingDirectory(
    val path: String? = null,
    val reason: String = "working directory missing"
)

@Serializable
public data class RunStatus(
    val runId: String,
    val status: RunStatusValue,
    val exitCode: Int? = null,
    val pid: Long? = null,
    val startedAtEpochMs: Long,
    val endedAtEpochMs: Long? = null,
    val lastSeq: Long = 0L
)

@Serializable
public data class RunCancelResponse(
    val accepted: Boolean,
    val status: RunStatusValue,
    val message: String? = null
)

@Serializable
public data class ActiveRuns(
    val runIds: List<String>,
    val message: String? = null
)

public sealed interface RunStartOutcome {
    public data class Created(val handle: RunHandle) : RunStartOutcome
    public data class Existing(val handle: RunHandle) : RunStartOutcome
    public data class Conflict(val conflict: RunConflict) : RunStartOutcome
    public data class InvalidCwd(val error: InvalidWorkingDirectory) : RunStartOutcome
    public data class ToolchainNotReady(val state: io.ltirom.server.toolchain.ToolchainState, val errorCode: String) : RunStartOutcome

    /** The service accepted a shutdown and no longer admits runs; start it again (or a newer one). */
    public data object ShuttingDown : RunStartOutcome
}

@Serializable
public data class PersistedRunMetadata(
    val runId: String,
    val request: StartRunRequest,
    val workingDirectory: String,
    val workspaceLock: String? = null,
    val idempotencyKey: String,
    val status: RunStatusValue,
    val pid: Long? = null,
    val exitCode: Int? = null,
    val startedAtEpochMs: Long,
    val endedAtEpochMs: Long? = null,
    val lastSeq: Long = 0L
)

