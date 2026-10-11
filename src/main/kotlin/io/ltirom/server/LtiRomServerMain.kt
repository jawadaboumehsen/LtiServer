package io.ltirom.server

import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.cio.*
import io.ktor.server.engine.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.cors.routing.*
import io.ktor.server.websocket.*
import io.ltirom.server.core.LinuxProcessRunner
import io.ltirom.server.engine.cancelWorkWithin
import io.ltirom.server.core.LinuxToolResolver
import io.ltirom.server.routing.ServerConfiguration
import io.ltirom.server.routing.configureRoutes
import kotlinx.serialization.json.Json
import java.io.File
import java.util.UUID
import kotlin.time.Duration.Companion.seconds

public fun main(args: Array<String>) {
    var port = 0
    var host = "0.0.0.0"
    var token: String? = null
    var toolsDir = File(System.getProperty("user.home"), "LtiRomTools/bin")
    var distroName = System.getenv("WSL_DISTRO_NAME") ?: "Ubuntu"

    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "--port", "-p" -> { if (i + 1 < args.size) port = args[++i].toInt() }
            "--host", "-h" -> { if (i + 1 < args.size) host = args[++i] }
            "--token", "-t" -> { if (i + 1 < args.size) token = args[++i] }
            "--tools-dir", "-d" -> { if (i + 1 < args.size) toolsDir = File(args[++i]) }
            "--distro" -> { if (i + 1 < args.size) distroName = args[++i] }
        }
        i++
    }

    if (token == null || token.isBlank()) {
        token = UUID.randomUUID().toString().replace("-", "")
    }

    val baseResolver = LinuxToolResolver(toolsBinDir = toolsDir)
    val toolResolver = io.ltirom.server.infra.DynamicToolRegistry(fallbackResolver = baseResolver)
    val processRunner = LinuxProcessRunner(resolver = toolResolver)

    val engine = io.ltirom.server.engine.DefaultServerEngine(
        toolResolver = toolResolver,
        processRunner = processRunner,
        distroName = distroName,
        sessionManager = io.ltirom.server.session.ExecutionSessionManager(autoRehydrate = true),
    )
    engine.addHold(io.ltirom.server.engine.Hold.ClientLifecycle)
    val watchdog = io.ltirom.server.engine.ClientWatchdog(
        cancelAllWork = {
            // Close the engine admission gate before collecting work. This is
            // atomic with run/direct-command admission, so no command can
            // appear in the check-to-cancel gap.
            val active = engine.closeAdmission(force = true)
            engine.cancelWorkWithin(active, CANCEL_BUDGET_MS)
            active
        },
        activeWorkProvider = {
            engine.activeWork()
        },
        onAdmissionOpened = {
            engine.reopenAdmission()
        }
    )

    val config = ServerConfiguration(
        authToken = token,
        distroName = distroName,
        engine = engine,
        toolResolver = toolResolver,
        watchdog = watchdog
    )

    val server = embeddedServer(CIO, port = port, host = host) {
        install(ContentNegotiation) {
            json(Json {
                ignoreUnknownKeys = true
                prettyPrint = true
                encodeDefaults = true
            })
        }
        install(WebSockets) {
            pingPeriod = 15.seconds
            timeout = 30.seconds
        }
        install(CORS) {
            anyHost()
            allowHeader(io.ktor.http.HttpHeaders.Authorization)
            allowHeader(io.ktor.http.HttpHeaders.ContentType)
        }
        configureRoutes(config)
    }

    server.start(wait = false)

    val actualPort = kotlinx.coroutines.runBlocking {
        server.engine.resolvedConnectors().first().port
    }
    val pid = ProcessHandle.current().pid()

    val stateStore = io.ltirom.server.infra.LockfileStateStore()
    stateStore.saveState(port = actualPort, token = token, pid = pid, distro = distroName)

    println("LTI_WSL_SERVER_READY port=$actualPort token=$token pid=$pid")
    System.out.flush()

    Runtime.getRuntime().addShutdownHook(Thread {
        stateStore.deleteState(pid)
        server.stop(1000, 2000)
    })

    Thread.currentThread().join()
}

private const val CANCEL_BUDGET_MS = 5_000L
