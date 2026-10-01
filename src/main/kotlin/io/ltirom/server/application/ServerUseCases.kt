package io.ltirom.server.application

import io.ltirom.server.domain.ports.ProcessCancellationPort
import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessOutputChunk
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.model.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File

/**
 * Application Use Case: Orchestrates the lookup and execution of a tool command.
 */
public class ExecuteToolUseCase(
    private val resolver: ToolResolverPort,
    private val processRunner: ProcessExecutionPort
) {
    public suspend fun execute(request: ToolExecutionRequest): ToolExecutionResponse {
        val resolved = resolver.resolve(request.toolId)
            ?: return ToolExecutionResponse(
                exitCode = -1,
                stdout = "",
                stderr = "Tool '${request.toolId}' not found or not executable",
                durationMs = 0L
            )

        val workingDir = request.workingDirectory?.let { File(it) }
        if (workingDir == null || !workingDir.isAbsolute || !workingDir.isDirectory) {
            return ToolExecutionResponse(
                exitCode = -1,
                stdout = "",
                stderr = "working directory missing",
                durationMs = 0L
            )
        }

        return try {
            val result = processRunner.runProcess(
                toolName = request.toolId,
                arguments = request.arguments,
                workingDir = workingDir,
                environment = request.environment,
                stdinText = request.stdinText,
                timeoutMs = request.timeoutMs
            )

            ToolExecutionResponse(
                exitCode = result.exitCode,
                stdout = result.stdout,
                stderr = result.stderr,
                durationMs = result.durationMs
            )
        } catch (e: Exception) {
            ToolExecutionResponse(
                exitCode = -1,
                stdout = "",
                stderr = e.message ?: "Execution error",
                durationMs = 0L
            )
        }
    }
}

/**
 * Application Use Case: Orchestrates real-time streaming of process output via reactive Flow.
 */
public class StreamToolUseCase(
    private val resolver: ToolResolverPort,
    private val processRunner: ProcessExecutionPort
) {
    public suspend fun stream(
        request: ToolExecutionRequest,
        emitEvent: suspend (StreamEvent) -> Unit
    ) {
        val resolved = resolver.resolve(request.toolId)
        if (resolved == null) {
            emitEvent(StreamEvent.OutputChunk("Tool '${request.toolId}' not found or not executable", isError = true))
            emitEvent(StreamEvent.ExecutionFinished(exitCode = -1, durationMs = 0L, summary = "Tool not found"))
            return
        }

        val workingDir = request.workingDirectory?.let { File(it) }
        if (workingDir == null || !workingDir.isAbsolute || !workingDir.isDirectory) {
            emitEvent(StreamEvent.OutputChunk("working directory missing", isError = true))
            emitEvent(StreamEvent.ExecutionFinished(exitCode = -1, durationMs = 0L, summary = "working directory missing"))
            return
        }

        try {
            processRunner.runProcessFlow(
                toolName = request.toolId,
                arguments = request.arguments,
                workingDir = workingDir,
                environment = request.environment,
                stdinText = request.stdinText,
                timeoutMs = request.timeoutMs
            ).collect { chunk ->
                when (chunk) {
                    is ProcessOutputChunk.Stdout -> emitEvent(StreamEvent.OutputChunk(chunk.line, isError = false))
                    is ProcessOutputChunk.Stderr -> emitEvent(StreamEvent.OutputChunk(chunk.line, isError = true))
                    is ProcessOutputChunk.Exit -> emitEvent(StreamEvent.ExecutionFinished(exitCode = chunk.exitCode, durationMs = chunk.durationMs))
                }
            }
        } catch (e: Exception) {
            emitEvent(StreamEvent.OutputChunk(e.message ?: "Execution failed", isError = true))
            emitEvent(StreamEvent.ExecutionFinished(exitCode = -1, durationMs = 0L, summary = e.message))
        }
    }
}

/**
 * Application Use Case: Cancels an active remote process by its execution ID.
 */
public class CancelProcessUseCase(
    private val cancellationPort: ProcessCancellationPort
) {
    public fun cancel(executionId: String, signal: String = "SIGTERM"): Boolean {
        return cancellationPort.cancelProcess(executionId, signal)
    }
}

/**
 * Application Use Case: Path translation via wslpath.
 */
public class TranslatePathUseCase {
    public suspend fun translate(request: PathTranslationRequest): PathTranslationResponse {
        val flag = if (request.direction == PathTranslationDirection.WINDOWS_TO_WSL) "-u" else "-w"
        val translated = runCatching {
            withContext(Dispatchers.IO) {
                val pb = ProcessBuilder("wslpath", flag, request.path).start()
                val out = pb.inputStream.bufferedReader().readText().trim()
                pb.waitFor()
                out
            }
        }.getOrElse { request.path }

        return PathTranslationResponse(request.path, translated)
    }
}

/**
 * Application Use Case: Gathers system health and lists available tools.
 */
public class GetServerHealthUseCase(
    public val resolver: ToolResolverPort,
    private val distroName: String,
    private val startTimeEpochMs: Long = System.currentTimeMillis()
) {
    public fun getHealth(): WslServerInfo {
        val uptime = System.currentTimeMillis() - startTimeEpochMs
        val kernel = System.getProperty("os.version") ?: "unknown"
        val arch = System.getProperty("os.arch") ?: "unknown"
        val javaVer = System.getProperty("java.version") ?: "unknown"

        val available = resolver.listAvailableTools().filterValues { it }.keys.toList()
        val serverVersion = readServerVersion()

        return WslServerInfo(
            status = "UP",
            distro = distroName,
            kernelRelease = kernel,
            architecture = arch,
            javaVersion = javaVer,
            serverUptimeMs = uptime,
            availableTools = available,
            serverVersion = serverVersion
        )
    }

    private fun readServerVersion(): String? {
        return GetServerHealthUseCase::class.java.getResourceAsStream("/server-version.txt")
            ?.bufferedReader()
            ?.use { it.readText().trim() }
            ?.takeIf { it.isNotEmpty() }
    }

    public fun listTools(): List<ToolStatusInfo> {
        return resolver.listToolStatuses()
    }

    public fun refreshTools(): List<ToolStatusInfo> {
        (resolver as? io.ltirom.server.infra.DynamicToolRegistry)?.refresh()
        return listTools()
    }
}
