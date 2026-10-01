package io.ltirom.server.routing

import io.ktor.client.plugins.contentnegotiation.ContentNegotiation as ClientContentNegotiation
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation as ServerContentNegotiation
import io.ktor.server.testing.*
import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.domain.ports.ProcessOutputChunk
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.engine.DefaultServerEngine
import io.ltirom.server.engine.Hold
import io.ltirom.server.model.*
import io.ltirom.server.toolchain.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.File
import java.nio.file.Files
import kotlin.test.*

class ToolchainRoutingTest {
    private val testToken = "test-secret-bearer-token"
    private val json = Json { ignoreUnknownKeys = true }

    private class FakeResolver : ToolResolverPort {
        override fun resolve(toolName: String): File = File("/bin/$toolName")
        override fun listAvailableTools(): Map<String, Boolean> = mapOf("fastboot" to true)
    }

    private class FakeRunner : ProcessExecutionPort {
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
        ): Flow<ProcessOutputChunk> = emptyFlow()
    }

    private fun ApplicationTestBuilder.setupTestApp(
        toolchainLayoutV2: Boolean = true,
        activator: ToolchainActivator? = null,
        journal: ToolchainJournal? = null,
        stateMachine: ToolchainStateMachine? = null,
        engineConfig: ((DefaultServerEngine) -> Unit)? = null
    ) {
        val resolver = FakeResolver()
        val runner = FakeRunner()
        val engine = DefaultServerEngine(
            toolResolver = resolver,
            processRunner = runner,
            distroName = "Ubuntu-Test"
        )
        engineConfig?.invoke(engine)

        val config = ServerConfiguration(
            authToken = testToken,
            distroName = "Ubuntu-Test",
            engine = engine,
            toolchainLayoutV2 = toolchainLayoutV2,
            activator = activator,
            journal = journal,
            stateMachine = stateMachine,
            toolResolver = resolver
        )

        application {
            install(ServerContentNegotiation) {
                json(Json { ignoreUnknownKeys = true; prettyPrint = true; encodeDefaults = true })
            }
            install(io.ktor.server.websocket.WebSockets)
            configureRoutes(config)
        }
    }

    @Test
    fun `toolchain endpoints return 404 when toolchainLayoutV2 is false`() = testApplication {
        setupTestApp(toolchainLayoutV2 = false)
        val client = createClient {
            install(ClientContentNegotiation) { json() }
        }

        val getResponse = client.get("/api/v1/toolchain/state") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.NotFound, getResponse.status)

        val postResponse = client.post("/api/v1/toolchain/activate") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(ActivateToolchainRequest("req-1", "i-1", "i-2"))
        }
        assertEquals(HttpStatusCode.NotFound, postResponse.status)

        val getActResponse = client.get("/api/v1/toolchain/activations/req-1") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.NotFound, getActResponse.status)
    }

    @Test
    fun `GET toolchain state returns current state machine and engine holds`() = testApplication {
        val tempDir = Files.createTempDirectory("toolchain-state-test").toFile()
        val journal = ToolchainJournal(tempDir)
        val stateMachine = MutableToolchainStateMachine(initialState = ToolchainState.READY)
        setupTestApp(
            toolchainLayoutV2 = true,
            journal = journal,
            stateMachine = stateMachine,
            engineConfig = { engine ->
                engine.addHold(Hold.Shutdown)
            }
        )
        val client = createClient {
            install(ClientContentNegotiation) { json() }
        }

        val response = client.get("/api/v1/toolchain/state") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.OK, response.status)

        val stateResp = json.decodeFromString<ToolchainStateResponse>(response.bodyAsText())
        assertEquals("READY", stateResp.state)
        assertTrue(stateResp.holds.contains("SHUTDOWN"))
        assertNotNull(stateResp.health)
        assertTrue(stateResp.health!!.resolves)
    }

    private class FakeActivator(
        toolsDir: File,
        stateDir: File,
        journal: ToolchainJournal,
        private val outcome: ActivationOutcome
    ) : ToolchainActivator(toolsDir, stateDir, journal) {
        override fun activate(
            requestId: String,
            expectedActive: String?,
            targetInstallId: String
        ): ActivationOutcome = outcome
    }

    @Test
    fun `POST activate rejects null or blank targetInstallId with 422 INVALID_TARGET`() = testApplication {
        setupTestApp(toolchainLayoutV2 = true)
        val client = createClient {
            install(ClientContentNegotiation) { json() }
        }

        val response = client.post("/api/v1/toolchain/activate") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(ActivateToolchainRequest(activationRequestId = "req-1", targetInstallId = null))
        }
        assertEquals(HttpStatusCode.UnprocessableEntity, response.status)
        val body = json.parseToJsonElement(response.bodyAsText()).jsonObject
        assertEquals("INVALID_TARGET", body["code"]?.jsonPrimitive?.content)
    }

    @Test
    fun `POST activate returns 200 COMMITTED when activator commits`() = testApplication {
        val tempDir = Files.createTempDirectory("act-succ-test").toFile()
        val journal = ToolchainJournal(tempDir)
        val activator = FakeActivator(tempDir, tempDir, journal, ActivationOutcome.Committed("i-target-99"))
        setupTestApp(toolchainLayoutV2 = true, activator = activator, journal = journal)
        val client = createClient {
            install(ClientContentNegotiation) { json() }
        }

        val response = client.post("/api/v1/toolchain/activate") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(ActivateToolchainRequest("req-succ", "i-old", "i-target-99"))
        }
        assertEquals(HttpStatusCode.OK, response.status)
        val body = json.parseToJsonElement(response.bodyAsText()).jsonObject
        assertEquals("COMMITTED", body["state"]?.jsonPrimitive?.content)
        assertEquals("i-target-99", body["activeInstallId"]?.jsonPrimitive?.content)
    }

    @Test
    fun `POST activate returns 202 IN_PROGRESS when activation is in progress`() = testApplication {
        val tempDir = Files.createTempDirectory("act-prog-test").toFile()
        val journal = ToolchainJournal(tempDir)
        val activator = FakeActivator(tempDir, tempDir, journal, ActivationOutcome.InProgress)
        setupTestApp(toolchainLayoutV2 = true, activator = activator, journal = journal)
        val client = createClient {
            install(ClientContentNegotiation) { json() }
        }

        val response = client.post("/api/v1/toolchain/activate") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(ActivateToolchainRequest("req-prog", "i-old", "i-target-99"))
        }
        assertEquals(HttpStatusCode.Accepted, response.status)
        val body = json.parseToJsonElement(response.bodyAsText()).jsonObject
        assertEquals("IN_PROGRESS", body["state"]?.jsonPrimitive?.content)
    }

    @Test
    fun `POST activate returns 409 CONFLICT on conflict`() = testApplication {
        val tempDir = Files.createTempDirectory("act-conf-test").toFile()
        val journal = ToolchainJournal(tempDir)
        val activator = FakeActivator(tempDir, tempDir, journal, ActivationOutcome.Rejected("CONFLICT", actualActiveInstallId = "i-actual-1"))
        setupTestApp(toolchainLayoutV2 = true, activator = activator, journal = journal)
        val client = createClient {
            install(ClientContentNegotiation) { json() }
        }

        val response = client.post("/api/v1/toolchain/activate") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(ActivateToolchainRequest("req-conf", "i-wrong", "i-target-99"))
        }
        assertEquals(HttpStatusCode.Conflict, response.status)
        val body = json.parseToJsonElement(response.bodyAsText()).jsonObject
        assertEquals("CONFLICT", body["code"]?.jsonPrimitive?.content)
        assertEquals("i-actual-1", body["actual"]?.jsonPrimitive?.content)
    }

    @Test
    fun `POST activate returns 409 BLOCKED on active work`() = testApplication {
        val tempDir = Files.createTempDirectory("act-block-test").toFile()
        val journal = ToolchainJournal(tempDir)
        val activator = FakeActivator(tempDir, tempDir, journal, ActivationOutcome.Blocked(listOf("run-123")))
        setupTestApp(toolchainLayoutV2 = true, activator = activator, journal = journal)
        val client = createClient {
            install(ClientContentNegotiation) { json() }
        }

        val response = client.post("/api/v1/toolchain/activate") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(ActivateToolchainRequest("req-block", "i-old", "i-target-99"))
        }
        assertEquals(HttpStatusCode.Conflict, response.status)
        val body = json.parseToJsonElement(response.bodyAsText()).jsonObject
        assertEquals("BLOCKED", body["code"]?.jsonPrimitive?.content)
    }

    @Test
    fun `POST activate returns 422 VERIFY_FAILED on verify failure restored`() = testApplication {
        val tempDir = Files.createTempDirectory("act-ver-test").toFile()
        val journal = ToolchainJournal(tempDir)
        val activator = FakeActivator(tempDir, tempDir, journal, ActivationOutcome.VerifyFailedRestored("Probe failed", "i-old"))
        setupTestApp(toolchainLayoutV2 = true, activator = activator, journal = journal)
        val client = createClient {
            install(ClientContentNegotiation) { json() }
        }

        val response = client.post("/api/v1/toolchain/activate") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(ActivateToolchainRequest("req-ver", "i-old", "i-target-99"))
        }
        assertEquals(HttpStatusCode.UnprocessableEntity, response.status)
        val body = json.parseToJsonElement(response.bodyAsText()).jsonObject
        assertEquals("VERIFY_FAILED", body["code"]?.jsonPrimitive?.content)
        assertEquals("Probe failed", body["reason"]?.jsonPrimitive?.content)
    }

    @Test
    fun `POST activate returns 503 MAINTENANCE_FAILED on maintenance failure`() = testApplication {
        val tempDir = Files.createTempDirectory("act-maint-test").toFile()
        val journal = ToolchainJournal(tempDir)
        val activator = FakeActivator(tempDir, tempDir, journal, ActivationOutcome.MaintenanceFailed("Restore also failed"))
        setupTestApp(toolchainLayoutV2 = true, activator = activator, journal = journal)
        val client = createClient {
            install(ClientContentNegotiation) { json() }
        }

        val response = client.post("/api/v1/toolchain/activate") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(ActivateToolchainRequest("req-maint", "i-old", "i-target-99"))
        }
        assertEquals(HttpStatusCode.ServiceUnavailable, response.status)
        val body = json.parseToJsonElement(response.bodyAsText()).jsonObject
        assertEquals("MAINTENANCE_FAILED", body["code"]?.jsonPrimitive?.content)
        assertEquals("Restore also failed", body["reason"]?.jsonPrimitive?.content)
    }

    @Test
    fun `GET activation by id returns 404 when record does not exist`() = testApplication {
        val tempDir = Files.createTempDirectory("toolchain-act-test").toFile()
        val journal = ToolchainJournal(tempDir)
        setupTestApp(toolchainLayoutV2 = true, journal = journal)
        val client = createClient {
            install(ClientContentNegotiation) { json() }
        }

        val response = client.get("/api/v1/toolchain/activations/req-missing") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.NotFound, response.status)
    }

    @Test
    fun `GET activation by id returns 200 with stored record when present`() = testApplication {
        val tempDir = Files.createTempDirectory("toolchain-act-found-test").toFile()
        val journal = ToolchainJournal(tempDir)
        journal.append(
            requestId = "req-found-1",
            type = JournalEntryType.REQUEST,
            payload = """{"from":"i-1","to":"i-2","requestHash":"hash1"}"""
        )
        journal.append(
            requestId = "req-found-1",
            type = JournalEntryType.COMMITTED,
            payload = """{"activeInstallId":"i-2","requestHash":"hash1"}"""
        )

        setupTestApp(toolchainLayoutV2 = true, journal = journal)
        val client = createClient {
            install(ClientContentNegotiation) { json() }
        }

        val response = client.get("/api/v1/toolchain/activations/req-found-1") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.OK, response.status)
        val record = json.decodeFromString<ActivationRecord>(response.bodyAsText())
        assertEquals("req-found-1", record.requestId)
        assertEquals(ActivationState.COMMITTED, record.state)
        assertEquals("i-2", record.activeInstallId)
    }
}
