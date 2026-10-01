package io.ltirom.server.routing

import io.ktor.client.plugins.contentnegotiation.ContentNegotiation as ClientContentNegotiation
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation as ServerContentNegotiation
import io.ktor.server.testing.*
import io.ltirom.server.domain.ports.*
import io.ltirom.server.model.*
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.serialization.json.Json
import java.io.File
import kotlin.test.*

class RunsEndpointTest {
    private val testToken = "test-secret-bearer-token"
    private val json = Json { ignoreUnknownKeys = true }

    private class ControlledFakeRunner : ProcessExecutionPort {
        val processStarted = CompletableDeferred<Unit>()
        val allowComplete = CompletableDeferred<Unit>()
        var capturedExecutionId: String? = null

        override suspend fun runProcess(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?
        ): ProcessExecutionResult {
            return ProcessExecutionResult(0, "ok", "", 10L)
        }

        override fun runProcessFlow(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            executionId: String?
        ): Flow<ProcessOutputChunk> = flow {
            capturedExecutionId = executionId
            processStarted.complete(Unit)
            allowComplete.await()
            emit(ProcessOutputChunk.Stdout("tool output"))
            emit(ProcessOutputChunk.Exit(0, 20L))
        }
    }

    private class FakeResolver : ToolResolverPort {
        override fun resolve(toolName: String): File = File("/bin/$toolName")
        override fun listAvailableTools(): Map<String, Boolean> = mapOf("test-tool" to true)
    }

    private fun ApplicationTestBuilder.setupTestApp(
        runner: ProcessExecutionPort = ControlledFakeRunner(),
        resolver: ToolResolverPort = FakeResolver()
    ) {
        val config = ServerConfiguration(
            authToken = testToken,
            distroName = "Ubuntu-Test",
            toolResolver = resolver,
            processRunner = runner
        )
        application {
            install(ServerContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
            install(io.ktor.server.websocket.WebSockets)
            configureRoutes(config)
        }
    }

    @Test
    fun `POST runs returns 201 with runId before process starts and reaches runner`() = testApplication {
        val runner = ControlledFakeRunner()
        setupTestApp(runner)

        val client = createClient {
            install(ClientContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
        }

        val testDir = File(".").absoluteFile
        val response = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(
                StartRunRequest(
                    request = ToolExecutionRequest(
                        toolId = "test-tool",
                        arguments = listOf("arg1"),
                        workingDirectory = testDir.absolutePath
                    ),
                    idempotencyKey = "key-create-1"
                )
            )
        }

        assertEquals(HttpStatusCode.Created, response.status)
        val handle = json.decodeFromString<RunHandle>(response.bodyAsText())
        assertTrue(handle.runId.isNotBlank())
        assertEquals(RunStatusValue.QUEUED, handle.status)

        runner.processStarted.await()
        assertEquals(handle.runId, runner.capturedExecutionId)
        runner.allowComplete.complete(Unit)
    }

    @Test
    fun `same idempotencyKey returns 200 with existing handle`() = testApplication {
        val runner = ControlledFakeRunner()
        setupTestApp(runner)

        val client = createClient {
            install(ClientContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
        }

        val testDir = File(".").absoluteFile
        val request = StartRunRequest(
            request = ToolExecutionRequest(
                toolId = "test-tool",
                arguments = listOf("arg1"),
                workingDirectory = testDir.absolutePath
            ),
            idempotencyKey = "idem-key-42"
        )

        val res1 = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(request)
        }
        assertEquals(HttpStatusCode.Created, res1.status)
        val handle1 = json.decodeFromString<RunHandle>(res1.bodyAsText())

        val res2 = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(request)
        }
        assertEquals(HttpStatusCode.OK, res2.status)
        val handle2 = json.decodeFromString<RunHandle>(res2.bodyAsText())
        assertEquals(handle1.runId, handle2.runId)

        runner.allowComplete.complete(Unit)
    }

    @Test
    fun `second run with same workspaceLock while RUNNING returns 409`() = testApplication {
        val runner = ControlledFakeRunner()
        setupTestApp(runner)

        val client = createClient {
            install(ClientContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
        }

        val testDir = File(".").absoluteFile
        val res1 = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(
                StartRunRequest(
                    request = ToolExecutionRequest(
                        toolId = "test-tool",
                        arguments = listOf("arg1"),
                        workingDirectory = testDir.absolutePath
                    ),
                    idempotencyKey = "lock-key-1",
                    workspaceLock = "/ws/isolated-lock"
                )
            )
        }
        assertEquals(HttpStatusCode.Created, res1.status)
        val handle1 = json.decodeFromString<RunHandle>(res1.bodyAsText())

        // Wait until runner starts so status becomes RUNNING
        runner.processStarted.await()

        val res2 = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(
                StartRunRequest(
                    request = ToolExecutionRequest(
                        toolId = "test-tool",
                        arguments = listOf("arg2"),
                        workingDirectory = testDir.absolutePath
                    ),
                    idempotencyKey = "lock-key-2",
                    workspaceLock = "/ws/isolated-lock"
                )
            )
        }
        assertEquals(HttpStatusCode.Conflict, res2.status)
        val conflict = json.decodeFromString<RunConflict>(res2.bodyAsText())
        assertEquals(handle1.runId, conflict.existingRunId)

        runner.allowComplete.complete(Unit)
    }

    @Test
    fun `invalid cwd returns 422 Unprocessable Entity`() = testApplication {
        val runner = ControlledFakeRunner()
        setupTestApp(runner)

        val client = createClient {
            install(ClientContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
        }

        // 1. Missing / null cwd
        val resNull = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(
                StartRunRequest(
                    request = ToolExecutionRequest(
                        toolId = "test-tool",
                        arguments = listOf("arg1"),
                        workingDirectory = null
                    ),
                    idempotencyKey = "invalid-cwd-1"
                )
            )
        }
        assertEquals(HttpStatusCode(422, "Unprocessable Entity"), resNull.status)
        val errNull = json.decodeFromString<InvalidWorkingDirectory>(resNull.bodyAsText())
        assertNull(errNull.path)

        // 2. Relative cwd
        val resRel = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(
                StartRunRequest(
                    request = ToolExecutionRequest(
                        toolId = "test-tool",
                        arguments = listOf("arg1"),
                        workingDirectory = "relative/path"
                    ),
                    idempotencyKey = "invalid-cwd-2"
                )
            )
        }
        assertEquals(HttpStatusCode(422, "Unprocessable Entity"), resRel.status)

        // 3. Non-existent cwd
        val resNonExist = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(
                StartRunRequest(
                    request = ToolExecutionRequest(
                        toolId = "test-tool",
                        arguments = listOf("arg1"),
                        workingDirectory = "/non/existent/path/999888777"
                    ),
                    idempotencyKey = "invalid-cwd-3"
                )
            )
        }
        assertEquals(HttpStatusCode(422, "Unprocessable Entity"), resNonExist.status)
    }

    @Test
    fun `GET runs by id and GET runs list query`() = testApplication {
        val runner = ControlledFakeRunner()
        setupTestApp(runner)

        val client = createClient {
            install(ClientContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
        }

        val testDir = File(".").absoluteFile
        val createRes = client.post("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(
                StartRunRequest(
                    request = ToolExecutionRequest(
                        toolId = "test-tool",
                        arguments = listOf("arg1"),
                        workingDirectory = testDir.absolutePath
                    ),
                    idempotencyKey = "list-test-key",
                    workspaceLock = "/ws/list-test"
                )
            )
        }
        assertEquals(HttpStatusCode.Created, createRes.status)
        val handle = json.decodeFromString<RunHandle>(createRes.bodyAsText())

        // GET /api/v1/runs/{id}
        val getRes = client.get("/api/v1/runs/${handle.runId}") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.OK, getRes.status)
        val runStatus = json.decodeFromString<RunStatus>(getRes.bodyAsText())
        assertEquals(handle.runId, runStatus.runId)

        // GET /api/v1/runs
        val listRes = client.get("/api/v1/runs") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.OK, listRes.status)
        val runsList = json.decodeFromString<List<RunStatus>>(listRes.bodyAsText())
        assertTrue(runsList.any { it.runId == handle.runId })

        // GET /api/v1/runs with workspaceLock filter
        val filterRes = client.get("/api/v1/runs?workspaceLock=/ws/list-test") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.OK, filterRes.status)
        val filteredList = json.decodeFromString<List<RunStatus>>(filterRes.bodyAsText())
        assertTrue(filteredList.any { it.runId == handle.runId })

        runner.allowComplete.complete(Unit)
    }
}
