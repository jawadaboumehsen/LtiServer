package io.ltirom.server.routing

import io.ltirom.server.engine.cancelWorkAndWait
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.websocket.*
import io.ltirom.server.application.*
import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.model.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import io.ltirom.server.infra.resolveTraceId

import io.ktor.utils.io.jvm.javaio.copyTo


public class ServerConfiguration(
    public val authToken: String,
    public val distroName: String,
    public val engine: io.ltirom.server.engine.LtiRomServerEngine,
    public val allowedBaseDirs: List<java.io.File>? = null,
    public val heartbeatIntervalMs: Long = 15_000L,
    public val exitOnShutdown: Boolean = true,
    public val toolResolver: ToolResolverPort? = null,
    public val watchdog: io.ltirom.server.engine.ClientWatchdog? = null
) {
    public constructor(
        authToken: String,
        distroName: String,
        toolResolver: ToolResolverPort,
        processRunner: ProcessExecutionPort,
        startTimeEpochMs: Long = System.currentTimeMillis(),
        sessionManager: io.ltirom.server.session.ExecutionSessionManager = io.ltirom.server.session.ExecutionSessionManager(),
        allowedBaseDirs: List<java.io.File>? = null,
        heartbeatIntervalMs: Long = 15_000L,
        exitOnShutdown: Boolean = true,
        watchdog: io.ltirom.server.engine.ClientWatchdog? = null
    ) : this(
        authToken = authToken,
        distroName = distroName,
        engine = io.ltirom.server.engine.DefaultServerEngine(
            toolResolver = toolResolver,
            processRunner = processRunner,
            distroName = distroName,
            startTimeEpochMs = startTimeEpochMs,
            sessionManager = sessionManager
        ),
        allowedBaseDirs = allowedBaseDirs,
        heartbeatIntervalMs = heartbeatIntervalMs,
        exitOnShutdown = exitOnShutdown,
        toolResolver = toolResolver,
        watchdog = watchdog
    )

    public constructor(
        authToken: String,
        distroName: String,
        executeToolUseCase: ExecuteToolUseCase,
        streamToolUseCase: StreamToolUseCase,
        translatePathUseCase: TranslatePathUseCase = TranslatePathUseCase(),
        healthUseCase: GetServerHealthUseCase,
        cancelProcessUseCase: CancelProcessUseCase? = null,
        allowedBaseDirs: List<java.io.File>? = null,
        heartbeatIntervalMs: Long = 15_000L,
        exitOnShutdown: Boolean = true,
        toolResolver: ToolResolverPort? = null,
        watchdog: io.ltirom.server.engine.ClientWatchdog? = null
    ) : this(
        authToken = authToken,
        distroName = distroName,
        engine = io.ltirom.server.engine.DefaultServerEngine(
            executeToolUseCase = executeToolUseCase,
            streamToolUseCase = streamToolUseCase,
            cancelProcessUseCase = cancelProcessUseCase,
            translatePathUseCase = translatePathUseCase,
            healthUseCase = healthUseCase
        ),
        allowedBaseDirs = allowedBaseDirs,
        heartbeatIntervalMs = heartbeatIntervalMs,
        exitOnShutdown = exitOnShutdown,
        toolResolver = toolResolver,
        watchdog = watchdog
    )
}



private fun expandHome(pathStr: String): String {
    val home = System.getenv("HOME") ?: System.getProperty("user.home")
    return when {
        pathStr == "~" -> home
        pathStr.startsWith("~/") -> home + pathStr.substring(1)
        pathStr.startsWith("~\\") -> home + pathStr.substring(1)
        else -> pathStr
    }
}

/** Constant-time comparison, so the token cannot be guessed from how long a wrong one takes to be refused. */
private fun sameToken(given: String?, expected: String): Boolean =
    given != null && java.security.MessageDigest.isEqual(given.toByteArray(), expected.toByteArray())

private fun resolveAllowedRoots(configuredDirs: List<java.io.File>?): List<java.nio.file.Path> {
    val homeDir = java.nio.file.Paths.get(
        System.getenv("HOME") ?: System.getProperty("user.home")
    ).toAbsolutePath().normalize()
    val homeReal = runCatching { homeDir.toRealPath() }.getOrDefault(homeDir)
    val baseRoots = configuredDirs?.map { it.toPath().toAbsolutePath().normalize() }
        ?: listOf(homeReal.resolve("LtiRomWorkDir"), homeReal.resolve(".ltirom"))
    return baseRoots.map { root ->
        if (!java.nio.file.Files.exists(root)) {
            runCatching { java.nio.file.Files.createDirectories(root) }
        }
        runCatching { root.toRealPath() }.getOrDefault(root)
    }
}

/**
 * HTTP and WebSocket Controller layer.
 * Adheres to Clean Architecture: routes act as pure adapters delegating
 * directly to the respective Application Use Cases.
 */
public fun Application.configureRoutes(config: ServerConfiguration) {
    val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
    val allowedRoots = resolveAllowedRoots(config.allowedBaseDirs)

    routing {
        // Unauthenticated health check endpoint
        get("/api/v1/health") {
            val protocolVersion = call.request.header("X-LtiRom-Protocol-Version")
            if (protocolVersion != null && protocolVersion != "1.1.0" && protocolVersion != "1.2.0") {
                call.response.headers.append("X-LtiRom-Protocol-Version", "1.2.0")
                call.respond(HttpStatusCode.BadRequest, "Unsupported protocol version: $protocolVersion (supported: 1.1.0, 1.2.0)")
                return@get
            }
            call.response.headers.append("X-LtiRom-Protocol-Version", "1.2.0")
            call.respond(config.engine.getHealth())
        }

        // Authenticated routes
        route("/api/v1") {
            intercept(ApplicationCallPipeline.Plugins) {
                call.response.headers.append("X-LtiRom-Protocol-Version", "1.2.0")
                call.resolveTraceId()
                if (call.request.uri.contains("/tools/stream") ||
                    (call.request.uri.contains("/tools/execute/") && call.request.uri.endsWith("/cancel"))) {
                    call.response.headers.append("Deprecation", "true")
                }
                // The one open endpoint, matched on the parsed path only. (Testing the raw URI's suffix also matched
                // "...?x=/health" and let any endpoint be called without a token.)
                if (call.request.httpMethod == HttpMethod.Get && call.request.path() == "/api/v1/health") {
                    return@intercept
                }
                val protocolVersion = call.request.header("X-LtiRom-Protocol-Version")
                if (protocolVersion != null && protocolVersion != "1.1.0" && protocolVersion != "1.2.0") {
                    call.respond(HttpStatusCode.BadRequest, "Unsupported protocol version: $protocolVersion (supported: 1.1.0, 1.2.0)")
                    finish()
                    return@intercept
                }
                val header = call.request.header("Authorization")
                val tokenFromHeader = header?.removePrefix("Bearer ")?.trim()
                val tokenFromQuery = call.request.queryParameters["token"]
                val valid = sameToken(tokenFromHeader, config.authToken) || sameToken(tokenFromQuery, config.authToken)
                if (!valid) {
                    call.respond(HttpStatusCode.Unauthorized, "Invalid or missing Bearer authorization token")
                    finish()
                }
            }

            get("/tools") {
                call.respond(config.engine.listTools())
            }

            post("/tools/refresh") {
                call.respond(config.engine.refreshTools())
            }

            post("/client/heartbeat") {
                val ok = config.watchdog?.recordHeartbeat() ?: true
                if (ok) {
                    call.respond(HttpStatusCode.NoContent)
                } else {
                    val code = when (config.watchdog?.admissionState) {
                        io.ltirom.server.engine.WatchdogAdmissionState.STOPPING -> "SERVER_STOPPING"
                        io.ltirom.server.engine.WatchdogAdmissionState.CANCELLING -> "CANCELLATION_IN_PROGRESS"
                        else -> "HEARTBEAT_REJECTED"
                    }
                    call.response.header("X-LtiRom-Error-Code", code)
                    call.respond(HttpStatusCode.Conflict, "Cannot record heartbeat: $code")
                }
            }

            post("/client/stop") {
                val result = config.watchdog?.stop() ?: io.ltirom.server.engine.StopResult(stopped = true)
                call.respond(HttpStatusCode.OK, result)
            }

            post("/runs") {
                if (config.watchdog != null && !config.watchdog.isAdmissionOpen) {
                    val code = when (config.watchdog.admissionState) {
                        io.ltirom.server.engine.WatchdogAdmissionState.STOPPING -> "SERVER_STOPPING"
                        io.ltirom.server.engine.WatchdogAdmissionState.CANCELLING -> "CANCELLATION_IN_PROGRESS"
                        else -> "CLIENT_HEARTBEAT_REQUIRED"
                    }
                    call.response.header("X-LtiRom-Error-Code", code)
                    call.respond(HttpStatusCode.ServiceUnavailable, "Command admission is closed; client heartbeat required")
                    return@post
                }
                val request = call.receive<StartRunRequest>()
                when (val outcome = config.engine.startRun(request)) {
                    is RunStartOutcome.Created -> call.respond(HttpStatusCode.Created, outcome.handle)
                    is RunStartOutcome.Existing -> call.respond(HttpStatusCode.OK, outcome.handle)
                    is RunStartOutcome.Conflict -> call.respond(HttpStatusCode.Conflict, outcome.conflict)
                    is RunStartOutcome.InvalidCwd -> call.respond(HttpStatusCode(422, "Unprocessable Entity"), outcome.error)
                    RunStartOutcome.ShuttingDown ->
                        call.respond(HttpStatusCode.ServiceUnavailable, "The build service is shutting down")
                }
            }

            get("/runs/{runId}") {
                val runId = call.parameters["runId"].orEmpty()
                val run = config.engine.getRun(runId)
                if (run != null) {
                    call.respond(run)
                } else {
                    call.respond(HttpStatusCode.NotFound, "Run '$runId' not found")
                }
            }

            get("/runs") {
                val workspaceLock = call.request.queryParameters["workspaceLock"]
                call.respond(config.engine.listRuns(workspaceLock))
            }

            webSocket("/runs/{runId}/attach") {
                val runId = call.parameters["runId"].orEmpty()
                val fromSeq = call.request.queryParameters["fromSeq"]?.toLongOrNull() ?: 1L
                val run = config.engine.getRun(runId)
                if (run == null) {
                    close(CloseReason(CloseReason.Codes.NORMAL, "Run not found"))
                    return@webSocket
                }
                val attachFlow = config.engine.attachRun(runId, fromSeq)
                if (attachFlow == null) {
                    close(CloseReason(CloseReason.Codes.NORMAL, "Run session not found"))
                    return@webSocket
                }

                val heartbeatJob = launch {
                    while (isActive) {
                        delay(config.heartbeatIntervalMs)
                        val heartbeat = SequencedStreamEvent(0L, StreamEvent.Heartbeat(System.currentTimeMillis()))
                        send(Frame.Text(json.encodeToString<SequencedStreamEvent>(heartbeat)))
                    }
                }

                try {
                    attachFlow.collect { event ->
                        send(Frame.Text(json.encodeToString<SequencedStreamEvent>(event)))
                    }
                } finally {
                    heartbeatJob.cancel()
                    runCatching { close(CloseReason(CloseReason.Codes.NORMAL, "Completed")) }
                }
            }


            post("/runs/{runId}/cancel") {
                val runId = call.parameters["runId"].orEmpty()
                val signal = call.request.queryParameters["signal"] ?: "SIGTERM"
                val response = config.engine.cancelRun(runId, signal)
                call.respond(response)
            }


            post("/tools/execute") {
                if (config.watchdog != null && !config.watchdog.isAdmissionOpen) {
                    val code = when (config.watchdog.admissionState) {
                        io.ltirom.server.engine.WatchdogAdmissionState.STOPPING -> "SERVER_STOPPING"
                        io.ltirom.server.engine.WatchdogAdmissionState.CANCELLING -> "CANCELLATION_IN_PROGRESS"
                        else -> "CLIENT_HEARTBEAT_REQUIRED"
                    }
                    call.response.header("X-LtiRom-Error-Code", code)
                    call.respond(HttpStatusCode.ServiceUnavailable, "Command admission is closed; client heartbeat required")
                    return@post
                }
                val request = call.receive<ToolExecutionRequest>()
                val traceId = call.request.header("X-Trace-Id")
                val response = try {
                    config.engine.execute(request, traceId)
                } catch (e: io.ltirom.server.engine.ServiceShuttingDownException) {
                    return@post call.respond(HttpStatusCode.ServiceUnavailable, e.message.orEmpty())
                } catch (e: io.ltirom.server.engine.InvalidExecutionIdException) {
                    return@post call.respond(HttpStatusCode.BadRequest, e.message.orEmpty())
                } catch (e: io.ltirom.server.engine.DuplicateExecutionException) {
                    return@post call.respond(HttpStatusCode.Conflict, e.message.orEmpty())
                }
                call.respond(response)
            }

            post("/tools/execute/{executionId}/cancel") {
                // Deprecation header is already added by the route interceptor above for this URI pattern.
                call.application.environment.log.warn("POST /api/v1/tools/execute/{executionId}/cancel is deprecated in v1.2.0; use POST /api/v1/runs/{runId}/cancel instead")
                val executionId = call.parameters["executionId"]
                if (executionId.isNullOrBlank()) {
                    return@post call.respond(HttpStatusCode.BadRequest, "Missing executionId")
                }
                val signal = call.request.queryParameters["signal"] ?: "SIGTERM"
                // The legacy port is blocking; do not occupy the HTTP event loop while observing exit.
                val cancelled = kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.IO) {
                    config.engine.cancel(executionId, signal)
                }
                call.respond(ProcessCancellationResponse(
                    success = cancelled,
                    executionId = executionId,
                    message = if (cancelled) "Process $executionId and tracked descendants confirmed terminated" else "Termination of process $executionId not confirmed (unknown ID, cancellation failure, or acknowledgement deadline expired)"
                ))
            }

            post("/path/translate") {
                val request = call.receive<PathTranslationRequest>()
                val response = config.engine.translatePath(request)
                call.respond(response)
            }

            get("/sessions") {
                call.respond(config.engine.listSessions())
            }

            get("/sessions/{sessionId}") {
                val sessionId = call.parameters["sessionId"].orEmpty()
                val session = config.engine.getSession(sessionId)
                if (session != null) {
                    call.respond(session)
                } else {
                    call.respond(HttpStatusCode.NotFound, "Session '$sessionId' not found")
                }
            }

            get("/sessions/{sessionId}/replay") {
                val sessionId = call.parameters["sessionId"].orEmpty()
                val fromSeq = call.request.queryParameters["fromSeq"]?.toLongOrNull() ?: 0L
                call.respond(config.engine.replaySession(sessionId, fromSeq))
            }

            webSocket("/tools/stream") {
                if (config.watchdog != null && !config.watchdog.isAdmissionOpen) {
                    val reasonText = when (config.watchdog.admissionState) {
                        io.ltirom.server.engine.WatchdogAdmissionState.STOPPING -> "SERVER_STOPPING"
                        io.ltirom.server.engine.WatchdogAdmissionState.CANCELLING -> "CANCELLATION_IN_PROGRESS"
                        else -> "CLIENT_HEARTBEAT_REQUIRED"
                    }
                    close(CloseReason(CloseReason.Codes.VIOLATED_POLICY, reasonText))
                    return@webSocket
                }
                call.application.environment.log.warn("WebSocket /api/v1/tools/stream is deprecated in v1.2.0; use POST /api/v1/runs and WS /api/v1/runs/{runId}/attach instead")
                val traceId = call.request.queryParameters["traceId"]
                val sessionId = call.request.queryParameters["sessionId"]
                val sequenced = call.request.queryParameters["sequenced"] == "true"
                for (frame in incoming) {
                    if (frame is Frame.Text) {
                        val text = frame.readText()
                        val req = runCatching { json.decodeFromString<ToolExecutionRequest>(text) }.getOrNull()
                        if (req != null) {
                            try {
                                config.engine.stream(req, traceId, sessionId).collect { seqEvent ->
                                    val payload = if (sequenced) {
                                        json.encodeToString<SequencedStreamEvent>(seqEvent)
                                    } else {
                                        json.encodeToString<StreamEvent>(seqEvent.event)
                                    }
                                    send(Frame.Text(payload))
                                }
                                close()
                            } catch (e: io.ltirom.server.engine.ServiceShuttingDownException) {
                                close(CloseReason(CloseReason.Codes.TRY_AGAIN_LATER, e.message.orEmpty()))
                            } catch (e: io.ltirom.server.engine.InvalidExecutionIdException) {
                                close(CloseReason(CloseReason.Codes.CANNOT_ACCEPT, e.message.orEmpty()))
                            } catch (e: io.ltirom.server.engine.DuplicateExecutionException) {
                                close(CloseReason(CloseReason.Codes.CANNOT_ACCEPT, e.message.orEmpty()))
                            }
                            return@webSocket
                        }
                    }
                }
            }

            post("/binary/upload") {
                val rawDestPath = call.request.queryParameters["path"]
                    ?: return@post call.respond(HttpStatusCode.BadRequest, "Missing 'path' query parameter")
                val destPath = expandHome(rawDestPath)
                // Decided without touching the disk: a refused request must not have created anything.
                val realTarget = io.ltirom.server.infra.planUpload(java.io.File(destPath), allowedRoots)
                    ?: return@post call.respond(HttpStatusCode.Forbidden, "PathNotAllowed")

                val startTime = System.currentTimeMillis()
                val channel = call.receiveChannel()
                val totalBytes = kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.IO) {
                    io.ltirom.server.infra.writeReplacing(realTarget) { out -> channel.copyTo(out) }
                }
                val duration = System.currentTimeMillis() - startTime
                call.respond(BinaryTransferResponse(path = rawDestPath, bytesTransferred = totalBytes, durationMs = duration))
            }

            get("/binary/download") {
                val rawSrcPath = call.request.queryParameters["path"]
                    ?: return@get call.respond(HttpStatusCode.BadRequest, "Missing 'path' query parameter")
                val srcPath = expandHome(rawSrcPath)
                val file = java.io.File(srcPath)
                if (!file.exists()) {
                    return@get call.respond(HttpStatusCode.NotFound, "File '$rawSrcPath' not found")
                }
                val realPath = runCatching { file.toPath().toRealPath() }.getOrNull()
                if (realPath == null || allowedRoots.none { realPath.startsWith(it) }) {
                    return@get call.respond(HttpStatusCode.Forbidden, "PathNotAllowed")
                }
                if (!realPath.toFile().isFile) {
                    return@get call.respond(HttpStatusCode.NotFound, "File '$rawSrcPath' not found")
                }
                call.respondFile(realPath.toFile())
            }

            post("/system/shutdown") {
                val force = call.request.queryParameters["force"]?.toBooleanStrictOrNull() ?: false
                // Closing admission and checking for active runs is one atomic step (see closeAdmission).
                val activeWork = config.engine.closeAdmission(force)
                if (activeWork.isNotEmpty() && !force) {
                    call.respond(
                        HttpStatusCode.Conflict,
                        ActiveRuns(runIds = activeWork, message = "Active runs prevent shutdown")
                    )
                    return@post
                }
                // Whatever is still running is stopped (and its process tree confirmed gone, within one limit) before
                // anything is promised: exiting first would leave the children running unmanaged. If some work
                // cannot be stopped the service stays up, admission stays closed, and the caller is told which.
                val survivors =
                    if (activeWork.isEmpty()) emptyList() else config.engine.cancelWorkAndWait(activeWork, SHUTDOWN_STOP_WAIT_MS)
                if (survivors.isNotEmpty()) {
                    config.watchdog?.shutdownRefused(survivors)
                    call.respond(
                        HttpStatusCode.Conflict,
                        ActiveRuns(runIds = survivors, message = "Work could not be stopped; the service keeps running")
                    )
                    return@post
                }
                call.respond(ShutdownResponse(status = "shutting down"))
                if (config.exitOnShutdown) {
                    @OptIn(kotlinx.coroutines.DelicateCoroutinesApi::class)
                    GlobalScope.launch {
                        delay(200)
                        kotlin.system.exitProcess(0)
                    }
                }
            }




        }
    }
}

private const val SHUTDOWN_STOP_WAIT_MS = 7_000L
