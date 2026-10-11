package io.ltirom.server.domain.ports

import io.ltirom.server.model.ToolStatusInfo
import java.io.File
import kotlinx.coroutines.flow.Flow

public data class ProcessExecutionResult(
    val exitCode: Int,
    val stdout: String,
    val stderr: String,
    val durationMs: Long,
    val survivors: List<Long> = emptyList()
)

public sealed interface ProcessOutputChunk {
    /** The process exists: who it is, so a restarted service can find it again. */
    public data class Started(val pid: Long, val startedAtEpochMs: Long?) : ProcessOutputChunk

    public data class Stdout(val line: String) : ProcessOutputChunk
    public data class Stderr(val line: String) : ProcessOutputChunk
    public data class Exit(
        val exitCode: Int,
        val durationMs: Long,
        val survivors: List<Long> = emptyList()
    ) : ProcessOutputChunk
}

/**
 * Port for resolving executable binaries on the server host.
 * Decouples use cases from file system paths and search algorithms.
 */
public interface ToolResolverPort {
    public fun resolve(toolName: String): File?
    public fun listAvailableTools(): Map<String, Boolean>
    public fun resolveSource(toolName: String): String? = null
    public fun listToolStatuses(): List<ToolStatusInfo> = listAvailableTools().map { (name, installed) ->
        ToolStatusInfo(
            tool = name,
            installed = installed,
            path = if (installed) resolve(name)?.absolutePath.orEmpty() else "",
            source = if (installed) resolveSource(name) else null,
            reason = null
        )
    }
}

/**
 * Port for executing system processes with resource limits, timeout, and stream monitoring.
 * Inverts the dependency on ProcessBuilder and POSIX signals.
 */
public interface ProcessExecutionPort {
    public suspend fun runProcess(
        toolName: String,
        arguments: List<String>,
        workingDir: File = File("."),
        environment: Map<String, String> = emptyMap(),
        stdinText: String? = null,
        timeoutMs: Long = 900_000L,
        onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)? = null,
        executionId: String? = null
    ): ProcessExecutionResult

    public fun runProcessFlow(
        toolName: String,
        arguments: List<String>,
        workingDir: File = File("."),
        environment: Map<String, String> = emptyMap(),
        stdinText: String? = null,
        timeoutMs: Long = 900_000L,
        executionId: String? = null
    ): Flow<ProcessOutputChunk> = kotlinx.coroutines.flow.flow {
        val result = runProcess(
            toolName = toolName,
            arguments = arguments,
            workingDir = workingDir,
            environment = environment,
            stdinText = stdinText,
            timeoutMs = timeoutMs,
            executionId = executionId
        )
        if (result.stdout.isNotEmpty()) {
            result.stdout.lines().forEach { emit(ProcessOutputChunk.Stdout(it)) }
        }
        if (result.stderr.isNotEmpty()) {
            result.stderr.lines().forEach { emit(ProcessOutputChunk.Stderr(it)) }
        }
        emit(ProcessOutputChunk.Exit(result.exitCode, result.durationMs))
    }
}

/**
 * Port for cancelling running processes by execution ID.
 * The blocking compatibility API returns true only after bounded observation confirms the
 * original process and its tracked descendants are terminal. Unknown IDs, failure and
 * expiry return false; successful signal dispatch alone is not an acknowledgement.
 */
public interface ProcessCancellationPort {
    public fun cancelProcess(executionId: String, signal: String = "SIGTERM"): Boolean
    public fun getSurvivors(executionId: String): List<Long> = emptyList()
}
