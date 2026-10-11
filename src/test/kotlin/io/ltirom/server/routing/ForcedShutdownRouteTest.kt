package io.ltirom.server.routing

import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.domain.ports.ProcessOutputChunk
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.engine.DefaultServerEngine
import io.ltirom.server.engine.LtiRomServerEngine
import io.ltirom.server.model.RunStartOutcome
import io.ltirom.server.model.StartRunRequest
import io.ltirom.server.model.ToolExecutionRequest
import io.ltirom.server.engine.ClientWatchdog
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** The forced shutdown route end to end: it only promises "shutting down" once the work is really gone. */
class ForcedShutdownRouteTest {
    private val token = "t"

    private class Resolver : ToolResolverPort {
        override fun resolve(toolName: String): File? = File("/fake/$toolName")

        override fun listAvailableTools(): Map<String, Boolean> = mapOf("sleep" to true)
    }

    private class Endless : ProcessExecutionPort {
        override suspend fun runProcess(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?,
            executionId: String?,
        ): ProcessExecutionResult = awaitCancellation()

        override fun runProcessFlow(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            executionId: String?,
        ): Flow<ProcessOutputChunk> = flow { awaitCancellation() }
    }

    /** Cannot be cancelled: runs until [release] is completed. */
    private class Stubborn(private val release: CompletableDeferred<Unit>) : ProcessExecutionPort by Endless() {
        override suspend fun runProcess(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?,
            executionId: String?,
        ): ProcessExecutionResult {
            withContext(NonCancellable) { release.await() }
            return ProcessExecutionResult(exitCode = 0, stdout = "", stderr = "", durationMs = 0)
        }
    }

    private fun request(key: String) = ToolExecutionRequest("sleep", emptyList(), workingDirectory = File(".").absolutePath)
        .let { StartRunRequest(it, key) }

    private fun ApplicationTestBuilder.app(engine: LtiRomServerEngine, watchdog: ClientWatchdog? = null) {
        val config = ServerConfiguration(
            authToken = token,
            distroName = "Ubuntu-Test",
            engine = engine,
            exitOnShutdown = false,
            watchdog = watchdog,
        )
        application {
            install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
            install(io.ktor.server.websocket.WebSockets)
            configureRoutes(config)
        }
    }

    @Test
    fun `work that stops lets the shutdown go ahead`() = testApplication {
        val engine = DefaultServerEngine(Resolver(), Endless(), "Ubuntu-Test")
        assertIs<RunStartOutcome.Created>(engine.startRun(request("a")))
        withTimeout(5_000) { while (engine.activeWork().isEmpty()) delay(10) }
        app(engine)

        val response =
            client.post("/api/v1/system/shutdown?force=true") { header(HttpHeaders.Authorization, "Bearer $token") }

        assertEquals(HttpStatusCode.OK, response.status)
        assertTrue(engine.activeWork().isEmpty(), "nothing may still run when the service says it is shutting down")
    }

    @Test
    fun `work that cannot be stopped is reported and the service does not claim to be shutting down`() =
        testApplication {
            val engine = DefaultServerEngine(Resolver(), Endless(), "Ubuntu-Test")
            // A legacy execution the runner cannot cancel: it stays tracked whatever is sent to it.
            val scope = CoroutineScope(Dispatchers.Default)
            val stuck = scope.launch { engine.execute(request("s").request, "stuck-1") }
            withTimeout(5_000) { while (engine.activeWork().isEmpty()) delay(10) }
            app(engine)

            val response =
                client.post("/api/v1/system/shutdown?force=true") { header(HttpHeaders.Authorization, "Bearer $token") }

            assertEquals(HttpStatusCode.Conflict, response.status)
            assertTrue("stuck-1" in response.bodyAsText(), "the survivors are named: ${response.bodyAsText()}")
            stuck.cancel()
        }

    // The wiring of LtiRomServerMain: the watchdog opens the engine's gate on an admission transition only.
    @Test
    fun `after a refused forced shutdown commands are admitted again once the survivors finish`() = testApplication {
        val release = CompletableDeferred<Unit>()
        val engine = DefaultServerEngine(Resolver(), Stubborn(release), "Ubuntu-Test")
        val watchdog = ClientWatchdog(
            activeWorkProvider = { engine.activeWork() },
            onAdmissionOpened = { engine.reopenAdmission() },
        )
        app(engine, watchdog)
        suspend fun heartbeat() =
            client.post("/api/v1/client/heartbeat") { header(HttpHeaders.Authorization, "Bearer $token") }.status
        assertEquals(HttpStatusCode.NoContent, heartbeat())

        val stuck = CoroutineScope(Dispatchers.Default).launch { engine.execute(request("s").request, "stuck-1") }
        withTimeout(5_000) { while (engine.activeWork().isEmpty()) delay(10) }
        val refused = client.post("/api/v1/system/shutdown?force=true") { header(HttpHeaders.Authorization, "Bearer $token") }
        assertEquals(HttpStatusCode.Conflict, refused.status)
        assertEquals(RunStartOutcome.ShuttingDown, engine.startRun(request("blocked")), "gate closed while survivors run")
        assertEquals(HttpStatusCode.Conflict, heartbeat(), "no reopening while the survivors are still running")

        release.complete(Unit)
        stuck.join()

        assertEquals(HttpStatusCode.NoContent, heartbeat())
        assertIs<RunStartOutcome.Created>(engine.startRun(request("after")), "admission recovers")
    }

    // The client waits 5 s for a normal shutdown (DaemonHealthProber.SHUTDOWN_TIMEOUT_MS); the service must answer
    // that request at once, because it never cancels or waits on the normal path.
    @Test
    fun `a normal shutdown with no work is answered at once`() = testApplication {
        app(DefaultServerEngine(Resolver(), Endless(), "Ubuntu-Test"))

        val start = System.nanoTime()
        val response = client.post("/api/v1/system/shutdown") { header(HttpHeaders.Authorization, "Bearer $token") }
        val elapsedMs = (System.nanoTime() - start) / 1_000_000

        assertEquals(HttpStatusCode.OK, response.status)
        assertTrue(elapsedMs < 1_000, "took $elapsedMs ms against the client's 5 s limit")
    }

    @Test
    fun `a normal shutdown with a run active is refused at once and leaves the run alone`() = testApplication {
        val engine = DefaultServerEngine(Resolver(), Endless(), "Ubuntu-Test")
        val runId = assertIs<RunStartOutcome.Created>(engine.startRun(request("a"))).handle.runId
        withTimeout(5_000) { while (engine.activeWork().isEmpty()) delay(10) }
        app(engine)

        val start = System.nanoTime()
        val response = client.post("/api/v1/system/shutdown") { header(HttpHeaders.Authorization, "Bearer $token") }
        val elapsedMs = (System.nanoTime() - start) / 1_000_000

        assertEquals(HttpStatusCode.Conflict, response.status)
        assertTrue(elapsedMs < 1_000, "took $elapsedMs ms against the client's 5 s limit")
        assertTrue(runId in engine.activeWork(), "a refused normal shutdown must not cancel the run")
    }
}
