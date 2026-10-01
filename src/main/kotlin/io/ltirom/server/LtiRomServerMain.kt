package io.ltirom.server

import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.cio.*
import io.ktor.server.engine.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.cors.routing.*
import io.ktor.server.websocket.*
import io.ltirom.server.core.LinuxProcessRunner
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
    var toolsDir = File(System.getProperty("user.home"), ".ltirom/bin")
    var distroName = System.getenv("WSL_DISTRO_NAME") ?: "Ubuntu"
    var toolchainLayoutV2 = false

    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "--port", "-p" -> { if (i + 1 < args.size) port = args[++i].toInt() }
            "--host", "-h" -> { if (i + 1 < args.size) host = args[++i] }
            "--token", "-t" -> { if (i + 1 < args.size) token = args[++i] }
            "--tools-dir", "-d" -> { if (i + 1 < args.size) toolsDir = File(args[++i]) }
            "--distro" -> { if (i + 1 < args.size) distroName = args[++i] }
            "--toolchain-layout-v2" -> {
                if (i + 1 < args.size && (args[i + 1] == "true" || args[i + 1] == "false")) {
                    toolchainLayoutV2 = args[++i].toBoolean()
                } else {
                    toolchainLayoutV2 = true
                }
            }
        }
        i++
    }

    if (toolchainLayoutV2) {
        val toolsHome = File(System.getProperty("user.home"), "LtiRomTools")
        val legacyBinDir = File(toolsHome, "bin")
        if (legacyBinDir.exists() && !java.nio.file.Files.isSymbolicLink(legacyBinDir.toPath())) {
            throw IllegalStateException(
                "Startup failed: ~/LtiRomTools/bin exists but is a directory, not a managed toolchain symlink. Unsupported legacy layout."
            )
        }
    }

    if (token == null || token.isBlank()) {
        token = UUID.randomUUID().toString().replace("-", "")
    }

    val baseResolver = LinuxToolResolver(toolsBinDir = toolsDir)
    val toolResolver = io.ltirom.server.infra.DynamicToolRegistry(fallbackResolver = baseResolver)
    val processRunner = LinuxProcessRunner(toolResolver)

    val stateDir = File(System.getProperty("user.home"), ".ltirom")
    val toolsHome = File(System.getProperty("user.home"), "LtiRomTools")
    val journal = if (toolchainLayoutV2) io.ltirom.server.toolchain.ToolchainJournal(stateDir) else null
    val stateMachine = if (journal != null) io.ltirom.server.toolchain.JournalToolchainStateMachine(journal) else null
    val engine = io.ltirom.server.engine.DefaultServerEngine(
        toolResolver = toolResolver,
        processRunner = processRunner,
        distroName = distroName,
        sessionManager = io.ltirom.server.session.ExecutionSessionManager(autoRehydrate = true),
        toolchainStateMachine = stateMachine
    )
    val osArch = System.getProperty("os.arch").lowercase()
    val arch = when {
        osArch == "amd64" || osArch == "x86_64" -> "x86_64"
        osArch == "aarch64" || osArch == "arm64" -> "aarch64"
        else -> osArch
    }
    val validator = io.ltirom.server.toolchain.InstallValidator(
        toolsDir = toolsHome,
        serviceDistro = distroName,
        serviceArch = arch
    )
    val activator = if (toolchainLayoutV2 && journal != null) {
        io.ltirom.server.toolchain.ToolchainActivator(
            toolsDir = toolsHome,
            stateDir = stateDir,
            journal = journal,
            validator = validator,
            tryAddHold = { engine.tryAddHold(it) },
            holdRemover = { engine.removeHold(it) },
            verifyProvider = { targetInstallId ->
                io.ltirom.server.toolchain.ToolchainActivator.verifyInstall(
                    toolsDir = toolsHome,
                    targetInstallId = targetInstallId,
                    registry = toolResolver,
                    runner = processRunner
                )
            }
        ).also { it.recoverStartup() }
    } else null

    val config = ServerConfiguration(
        authToken = token,
        distroName = distroName,
        engine = engine,
        toolchainLayoutV2 = toolchainLayoutV2,
        activator = activator,
        journal = journal,
        toolResolver = toolResolver
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
        stateStore.deleteState()
        server.stop(1000, 2000)
    })

    Thread.currentThread().join()
}
