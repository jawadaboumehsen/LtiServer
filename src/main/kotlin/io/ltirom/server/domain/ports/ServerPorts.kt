package io.ltirom.server.domain.ports

import io.ltirom.server.model.ToolStatusInfo
import java.io.File
import kotlinx.coroutines.flow.Flow

public data class ProcessExecutionResult(
    val exitCode: Int,
    val stdout: String,
    val stderr: String,
    val durationMs: Long
)

public sealed interface ProcessOutputChunk {
    public data class Stdout(val line: String) : ProcessOutputChunk
    public data class Stderr(val line: String) : ProcessOutputChunk
    public data class Exit(val exitCode: Int, val durationMs: Long) : ProcessOutputChunk
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
        onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)? = null
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
            timeoutMs = timeoutMs
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
 */
public interface ProcessCancellationPort {
    public fun cancelProcess(executionId: String, signal: String = "SIGTERM"): Boolean
}
