package io.ltirom.server.routing

import io.ktor.client.plugins.contentnegotiation.ContentNegotiation as ClientContentNegotiation
import io.ktor.client.plugins.websocket.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation as ServerContentNegotiation
import io.ktor.server.testing.*
import io.ktor.websocket.*
import io.ltirom.server.domain.ports.*
import io.ltirom.server.model.*
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import java.io.File
import kotlin.test.*

class AttachAndCancelTest {
    private val testToken = "test-attach-cancel-token"
    private val json = Json { ignoreUnknownKeys = true }

    private class TestProcessRunner : ProcessExecutionPort, ProcessCancellationPort {
        val processStarted = CompletableDeferred<Unit>()
        val allowComplete = CompletableDeferred<Unit>()
        val cancelCalled = CompletableDeferred<String>()
        var isProcessRunning = false

        override suspend fun runProcess(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?
        ): ProcessExecutionResult = ProcessExecutionResult(0, "ok", "", 10L)

        override fun runProcessFlow(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            executionId: String?
        ): Flow<ProcessOutputChunk> = flow {
            isProcessRunning = true
            processStarted.complete(Unit)
            emit(ProcessOutputChunk.Stdout("line 1"))
            emit(ProcessOutputChunk.Stdout("line 2"))
            allowComplete.await()
            emit(ProcessOutputChunk.Stdout("line 3"))
            emit(ProcessOutputChunk.Exit(0, 50L))
            isProcessRunning = false
        }

        override fun cancelProcess(executionId: String, signal: String): Boolean {
            cancelCalled.complete(signal)
            allowComplete.complete(Unit)
            return true
        }

    }

    private class FakeResolver : ToolResolverPort {
        override fun resolve(toolName: String): File = File("/bin/$toolName")
        override fun listAvailableTools(): Map<String, Boolean> = mapOf("test-tool" to true)
    }

    private fun ApplicationTestBuilder.setupTestApp(
        runner: TestProcessRunner = TestProcessRunner(),
        heartbeatIntervalMs: Long = 15_000L
    ): Pair<TestProcessRunner, ServerConfiguration> {
        val config = ServerConfiguration(
            authToken = testToken,
            distroName = "Ubuntu-Test",
            toolResolver = FakeResolver(),
            processRunner = runner,
            heartbeatIntervalMs = heartbeatIntervalMs,
            exitOnShutdown = false
        )

        application {
            install(ServerContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
            install(io.ktor.server.websocket.WebSockets)
            configureRoutes(config)
        }
        return Pair(runner, config)
    }

    @Test
    fun `attach replays then streams live with contiguous seq`() = testApplication {
        val (runner, _) = setupTestApp()
        val client = createClient {
            install(ClientContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
            install(io.ktor.client.plugins.websocket.WebSockets)
        }

        val testDir = File(".").absoluteFile
        val createRes = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(
                StartRunRequest(
                    request = ToolExecutionRequest(toolId = "test-tool", arguments = emptyList(), workingDirectory = testDir.absolutePath),
                    idempotencyKey = "attach-test-1"
                )
            )
        }
        val handle = json.decodeFromString<RunHandle>(createRes.bodyAsText())
        runner.processStarted.await()

        // Attach to the run with fromSeq = 1
        client.webSocket("/api/v1/runs/${handle.runId}/attach?fromSeq=1", {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }) {
            val events = mutableListOf<SequencedStreamEvent>()

            // First two lines were emitted during process start
            val f1 = (incoming.receive() as Frame.Text).readText()
            val e1 = json.decodeFromString<SequencedStreamEvent>(f1)
            events.add(e1)

            val f2 = (incoming.receive() as Frame.Text).readText()
            val e2 = json.decodeFromString<SequencedStreamEvent>(f2)
            events.add(e2)

            assertEquals(1L, e1.seq)
            assertEquals(2L, e2.seq)
            assertEquals("line 1", (e1.event as StreamEvent.OutputChunk).text)
            assertEquals("line 2", (e2.event as StreamEvent.OutputChunk).text)

            // Release the runner so it emits line 3 and finishes
            runner.allowComplete.complete(Unit)

            val f3 = (incoming.receive() as Frame.Text).readText()
            val e3 = json.decodeFromString<SequencedStreamEvent>(f3)
            events.add(e3)
            assertEquals(3L, e3.seq)
            assertEquals("line 3", (e3.event as StreamEvent.OutputChunk).text)

            val f4 = (incoming.receive() as Frame.Text).readText()
            val e4 = json.decodeFromString<SequencedStreamEvent>(f4)
            events.add(e4)
            assertEquals(4L, e4.seq)
            assertTrue(e4.event is StreamEvent.ExecutionFinished)
        }
    }

    @Test
    fun `socket close does not end the process`() = testApplication {
        val (runner, _) = setupTestApp()
        val client = createClient {
            install(ClientContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
            install(io.ktor.client.plugins.websocket.WebSockets)
        }

        val testDir = File(".").absoluteFile
        val createRes = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(
                StartRunRequest(
                    request = ToolExecutionRequest(toolId = "test-tool", arguments = emptyList(), workingDirectory = testDir.absolutePath),
                    idempotencyKey = "socket-close-test"
                )
            )
        }
        val handle = json.decodeFromString<RunHandle>(createRes.bodyAsText())
        runner.processStarted.await()

        // Connect, read one frame, then immediately close socket
        client.webSocket("/api/v1/runs/${handle.runId}/attach?fromSeq=1", {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }) {
            val f = incoming.receive()
            assertTrue(f is Frame.Text)
            close()
        }

        // Process must still be running in background!
        assertTrue(runner.isProcessRunning, "Process must remain running after socket closed")

        // Now allow process to complete
        runner.allowComplete.complete(Unit)

        // Give it a moment to finish
        withTimeout(2000) {
            while (runner.isProcessRunning) {
                kotlinx.coroutines.delay(20)
            }
        }
        assertFalse(runner.isProcessRunning)

        // Verify status is COMPLETED
        val getRes = client.get("/api/v1/runs/${handle.runId}") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        val status = json.decodeFromString<RunStatus>(getRes.bodyAsText())
        assertEquals(RunStatusValue.COMPLETED, status.status)
    }

    @Test
    fun `cancel transitions to CANCELLING then CANCELLED and ExecutionFinished does not overwrite`() = testApplication {
        val (runner, _) = setupTestApp()
        val client = createClient {
            install(ClientContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
        }

        val testDir = File(".").absoluteFile
        val createRes = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(
                StartRunRequest(
                    request = ToolExecutionRequest(toolId = "test-tool", arguments = emptyList(), workingDirectory = testDir.absolutePath),
                    idempotencyKey = "cancel-test-1"
                )
            )
        }
        val handle = json.decodeFromString<RunHandle>(createRes.bodyAsText())
        runner.processStarted.await()

        // Cancel the active run
        val cancelRes = client.post("/api/v1/runs/${handle.runId}/cancel?signal=SIGTERM") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.OK, cancelRes.status)
        val cancelBody = json.decodeFromString<RunCancelResponse>(cancelRes.bodyAsText())
        assertTrue(cancelBody.accepted)
        assertEquals(RunStatusValue.CANCELLING, cancelBody.status)

        // The runner completes (and emits exit code 0)
        runner.allowComplete.complete(Unit)

        withTimeout(2000) {
            while (runner.isProcessRunning) {
                kotlinx.coroutines.delay(20)
            }
        }

        // Even though exitCode was 0, final status must remain CANCELLED!
        val getRes = client.get("/api/v1/runs/${handle.runId}") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        val status = json.decodeFromString<RunStatus>(getRes.bodyAsText())
        assertEquals(RunStatusValue.CANCELLED, status.status)
    }

    @Test
    fun `cancel on terminal run returns accepted false and status unchanged`() = testApplication {
        val (runner, _) = setupTestApp()
        val client = createClient {
            install(ClientContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
        }

        val testDir = File(".").absoluteFile
        val createRes = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(
                StartRunRequest(
                    request = ToolExecutionRequest(toolId = "test-tool", arguments = emptyList(), workingDirectory = testDir.absolutePath),
                    idempotencyKey = "term-cancel-1"
                )
            )
        }
        val handle = json.decodeFromString<RunHandle>(createRes.bodyAsText())
        runner.processStarted.await()
        runner.allowComplete.complete(Unit)

        withTimeout(2000) {
            while (runner.isProcessRunning) {
                kotlinx.coroutines.delay(20)
            }
        }

        // Now the run is COMPLETED
        val cancelRes = client.post("/api/v1/runs/${handle.runId}/cancel") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        val cancelBody = json.decodeFromString<RunCancelResponse>(cancelRes.bodyAsText())
        assertFalse(cancelBody.accepted)
        assertEquals(RunStatusValue.COMPLETED, cancelBody.status)
    }

    @Test
    fun `two sockets may attach to one run and both receive identical sequenced frames`() = testApplication {
        val (runner, _) = setupTestApp()
        val client = createClient {
            install(ClientContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
            install(io.ktor.client.plugins.websocket.WebSockets)
        }

        val testDir = File(".").absoluteFile
        val createRes = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(
                StartRunRequest(
                    request = ToolExecutionRequest(toolId = "test-tool", arguments = emptyList(), workingDirectory = testDir.absolutePath),
                    idempotencyKey = "two-sockets-1"
                )
            )
        }
        val handle = json.decodeFromString<RunHandle>(createRes.bodyAsText())
        runner.processStarted.await()

        val frames1 = mutableListOf<SequencedStreamEvent>()
        val frames2 = mutableListOf<SequencedStreamEvent>()

        val s1Ready = CompletableDeferred<Unit>()
        val s2Ready = CompletableDeferred<Unit>()

        kotlinx.coroutines.coroutineScope {
            val j1 = launch {
                client.webSocket("/api/v1/runs/${handle.runId}/attach?fromSeq=1", {
                    header(HttpHeaders.Authorization, "Bearer $testToken")
                }) {
                    s1Ready.complete(Unit)
                    for (frame in incoming) {
                        if (frame is Frame.Text) {
                            val event = json.decodeFromString<SequencedStreamEvent>(frame.readText())
                            frames1.add(event)
                            if (event.event is StreamEvent.ExecutionFinished) break
                        }
                    }
                }
            }

            val j2 = launch {
                client.webSocket("/api/v1/runs/${handle.runId}/attach?fromSeq=1", {
                    header(HttpHeaders.Authorization, "Bearer $testToken")
                }) {
                    s2Ready.complete(Unit)
                    for (frame in incoming) {
                        if (frame is Frame.Text) {
                            val event = json.decodeFromString<SequencedStreamEvent>(frame.readText())
                            frames2.add(event)
                            if (event.event is StreamEvent.ExecutionFinished) break
                        }
                    }
                }
            }

            s1Ready.await()
            s2Ready.await()
            runner.allowComplete.complete(Unit)

            j1.join()
            j2.join()
        }


        assertEquals(frames1.size, frames2.size)
        assertEquals(frames1.map { it.seq }, frames2.map { it.seq })
    }

    @Test
    fun `shutdown returns 409 with active runs and 200 with force true`() = testApplication {
        val (runner, _) = setupTestApp()
        val client = createClient {
            install(ClientContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
        }

        val testDir = File(".").absoluteFile
        val createRes = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(
                StartRunRequest(
                    request = ToolExecutionRequest(toolId = "test-tool", arguments = emptyList(), workingDirectory = testDir.absolutePath),
                    idempotencyKey = "shutdown-test-1"
                )
            )
        }
        val handle = json.decodeFromString<RunHandle>(createRes.bodyAsText())
        runner.processStarted.await()

        // Shutdown without force -> 409 Conflict
        val shutRes1 = client.post("/api/v1/system/shutdown") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.Conflict, shutRes1.status)
        val activeRuns = json.decodeFromString<ActiveRuns>(shutRes1.bodyAsText())
        assertTrue(activeRuns.runIds.contains(handle.runId))

        // Shutdown with force=true -> 200 OK
        val shutRes2 = client.post("/api/v1/system/shutdown?force=true") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.OK, shutRes2.status)

        runner.allowComplete.complete(Unit)
    }

    @Test
    fun `heartbeat transmitted periodically`() = testApplication {
        // Configure fast heartbeat of 50ms for the test
        val (runner, _) = setupTestApp(heartbeatIntervalMs = 50L)
        val client = createClient {
            install(ClientContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
            install(io.ktor.client.plugins.websocket.WebSockets)
        }

        val testDir = File(".").absoluteFile
        val createRes = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(
                StartRunRequest(
                    request = ToolExecutionRequest(toolId = "test-tool", arguments = emptyList(), workingDirectory = testDir.absolutePath),
                    idempotencyKey = "heartbeat-test-1"
                )
            )
        }
        val handle = json.decodeFromString<RunHandle>(createRes.bodyAsText())
        runner.processStarted.await()

        client.webSocket("/api/v1/runs/${handle.runId}/attach?fromSeq=1", {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }) {
            // Collect until we see a heartbeat frame (seq == 0, event is Heartbeat)
            var heartbeatSeen = false
            withTimeout(2000) {
                for (frame in incoming) {
                    if (frame is Frame.Text) {
                        val event = json.decodeFromString<SequencedStreamEvent>(frame.readText())
                        if (event.seq == 0L && event.event is StreamEvent.Heartbeat) {
                            heartbeatSeen = true
                            break
                        }
                    }
                }
            }
            assertTrue(heartbeatSeen, "Must receive Heartbeat frame with seq = 0")
        }

        runner.allowComplete.complete(Unit)
    }
}
