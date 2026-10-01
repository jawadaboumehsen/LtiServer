package io.ltirom.server.routing

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

import io.ltirom.server.toolchain.*

public class ServerConfiguration(
    public val authToken: String,
    public val distroName: String,
    public val engine: io.ltirom.server.engine.LtiRomServerEngine,
    public val allowedBaseDirs: List<java.io.File>? = null,
    public val heartbeatIntervalMs: Long = 15_000L,
    public val exitOnShutdown: Boolean = true,
    public val toolchainLayoutV2: Boolean = false,
    public val activator: ToolchainActivator? = null,
    public val journal: ToolchainJournal? = null,
    public val stateMachine: ToolchainStateMachine? = null,
    public val toolResolver: ToolResolverPort? = null
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
        toolchainLayoutV2: Boolean = false,
        activator: ToolchainActivator? = null,
        journal: ToolchainJournal? = null,
        stateMachine: ToolchainStateMachine? = null
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
        toolchainLayoutV2 = toolchainLayoutV2,
        activator = activator,
        journal = journal,
        stateMachine = stateMachine,
        toolResolver = toolResolver
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
        toolchainLayoutV2: Boolean = false,
        activator: ToolchainActivator? = null,
        journal: ToolchainJournal? = null,
        stateMachine: ToolchainStateMachine? = null,
        toolResolver: ToolResolverPort? = null
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
        toolchainLayoutV2 = toolchainLayoutV2,
        activator = activator,
        journal = journal,
        stateMachine = stateMachine,
        toolResolver = toolResolver
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
                if (call.request.uri.endsWith("/health")) {
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
                val valid = (tokenFromHeader == config.authToken) || (tokenFromQuery == config.authToken)
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

            post("/runs") {
                val request = call.receive<StartRunRequest>()
                when (val outcome = config.engine.startRun(request)) {
                    is RunStartOutcome.Created -> call.respond(HttpStatusCode.Created, outcome.handle)
                    is RunStartOutcome.Existing -> call.respond(HttpStatusCode.OK, outcome.handle)
                    is RunStartOutcome.Conflict -> call.respond(HttpStatusCode.Conflict, outcome.conflict)
                    is RunStartOutcome.InvalidCwd -> call.respond(HttpStatusCode(422, "Unprocessable Entity"), outcome.error)
                    is RunStartOutcome.ToolchainNotReady ->
                        call.respond(HttpStatusCode.ServiceUnavailable, ProblemDetails(
                            type = "https://ltirom.io/errors/${outcome.errorCode.lowercase()}",
                            title = outcome.errorCode,
                            status = 503,
                            detail = "Toolchain is not ready: ${outcome.errorCode}"
                        ))
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
                val request = call.receive<ToolExecutionRequest>()
                val traceId = call.request.header("X-Trace-Id")
                val response = try {
                    config.engine.execute(request, traceId)
                } catch (e: io.ltirom.server.engine.ServiceShuttingDownException) {
                    return@post call.respond(HttpStatusCode.ServiceUnavailable, e.message.orEmpty())
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
                val cancelled = config.engine.cancel(executionId, signal)
                call.respond(ProcessCancellationResponse(
                    success = cancelled,
                    executionId = executionId,
                    message = if (cancelled) "Signal $signal dispatched to process $executionId" else "Process $executionId not found or already terminated"
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
                val targetFile = java.io.File(destPath)
                val parentFile = targetFile.parentFile ?: java.io.File(".")
                parentFile.mkdirs()

                val realParent = runCatching { parentFile.toPath().toRealPath() }.getOrNull()
                val realTarget = realParent?.resolve(targetFile.name)?.normalize()
                if (realTarget == null || allowedRoots.none { realTarget.startsWith(it) }) {
                    return@post call.respond(HttpStatusCode.Forbidden, "PathNotAllowed")
                }

                val startTime = System.currentTimeMillis()
                val channel = call.receiveChannel()
                val totalBytes = kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.IO) {
                    realTarget.toFile().outputStream().use { fos ->
                        channel.copyTo(fos)
                    }
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
                if (activeWork.isNotEmpty()) {
                    call.respond(
                        HttpStatusCode.Conflict,
                        ActiveRuns(runIds = activeWork, message = "Active runs prevent shutdown")
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

            if (config.toolchainLayoutV2) {
                route("/toolchain") {
                    get("/state") {
                        val currentJournal = config.journal?.fold()
                        val stateStr = config.stateMachine?.state?.name ?: currentJournal?.state?.name ?: "UNINITIALIZED"
                        val activeInstall = currentJournal?.activeInstallId
                        val previousInstall = currentJournal?.previousInstallId
                        val holds = config.engine.admissionHolds.map { it.label }
                        val lastAct = currentJournal?.activations?.values?.lastOrNull()?.let {
                            LastActivationSummary(it.requestId, it.state.name)
                        }
                        val health = ToolchainHealthSummary(
                            intact = true,
                            resolves = true
                        )
                        call.respond(
                            ToolchainStateResponse(
                                state = stateStr,
                                activeInstallId = activeInstall,
                                previousInstallId = previousInstall,
                                holds = holds,
                                lastActivation = lastAct,
                                health = health
                            )
                        )
                    }

                    post("/activate") {
                        val request = call.receive<ActivateToolchainRequest>()
                        if (request.targetInstallId.isNullOrBlank()) {
                            call.respond(
                                HttpStatusCode.UnprocessableEntity,
                                ActivateToolchainResponse(code = "INVALID_TARGET", reason = "targetInstallId cannot be null or blank")
                            )
                            return@post
                        }
                        val outcome = config.activator?.activate(
                            requestId = request.activationRequestId,
                            expectedActive = request.expectedActiveInstallId,
                            targetInstallId = request.targetInstallId
                        ) ?: ActivationOutcome.Rejected("ACTIVATOR_UNAVAILABLE")

                        when (outcome) {
                            is ActivationOutcome.Committed -> call.respond(
                                HttpStatusCode.OK,
                                ActivateToolchainResponse(state = "COMMITTED", activeInstallId = outcome.activeInstallId)
                            )
                            is ActivationOutcome.InProgress -> call.respond(
                                HttpStatusCode.Accepted,
                                ActivateToolchainResponse(state = "IN_PROGRESS")
                            )
                            is ActivationOutcome.Blocked -> call.respond(
                                HttpStatusCode.Conflict,
                                ActivateToolchainResponse(code = "BLOCKED", activeWork = outcome.activeWork.size)
                            )
                            is ActivationOutcome.Rejected -> {
                                if (outcome.code == "CONFLICT") {
                                    call.respond(
                                        HttpStatusCode.Conflict,
                                        ActivateToolchainResponse(code = "CONFLICT", actual = outcome.actualActiveInstallId, reason = outcome.reason)
                                    )
                                } else if (outcome.code == "BLOCKED") {
                                    call.respond(
                                        HttpStatusCode.Conflict,
                                        ActivateToolchainResponse(code = "BLOCKED")
                                    )
                                } else {
                                    call.respond(
                                        HttpStatusCode.UnprocessableEntity,
                                        ActivateToolchainResponse(code = outcome.code, reason = outcome.reason, toolIds = outcome.conflictingToolIds)
                                    )
                                }
                            }
                            is ActivationOutcome.VerifyFailedRestored -> call.respond(
                                HttpStatusCode.UnprocessableEntity,
                                ActivateToolchainResponse(code = "VERIFY_FAILED", reason = outcome.reason, actual = outcome.activeInstallId)
                            )
                            is ActivationOutcome.MaintenanceFailed -> call.respond(
                                HttpStatusCode.ServiceUnavailable,
                                ActivateToolchainResponse(code = "MAINTENANCE_FAILED", reason = outcome.reason)
                            )
                        }
                    }

                    get("/activations/{id}") {
                        val id = call.parameters["id"]
                        val record = if (id != null) config.journal?.fold()?.activations?.get(id) else null
                        if (record == null) {
                            call.respond(HttpStatusCode.NotFound)
                        } else {
                            call.respond(HttpStatusCode.OK, record)
                        }
                    }
                }
            }


        }
    }
}
