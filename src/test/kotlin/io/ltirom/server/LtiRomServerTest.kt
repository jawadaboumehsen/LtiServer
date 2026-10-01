package io.ltirom.server

import io.ktor.client.plugins.contentnegotiation.ContentNegotiation as ClientContentNegotiation
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation as ServerContentNegotiation
import io.ktor.server.testing.*
import io.ltirom.server.core.LinuxProcessRunner
import io.ltirom.server.core.LinuxToolResolver
import io.ltirom.server.model.*
import io.ltirom.server.routing.ServerConfiguration
import io.ltirom.server.routing.configureRoutes
import kotlinx.serialization.json.Json
import java.io.File
import kotlin.test.*

class LtiRomServerTest {
    private val testToken = "test-secret-bearer-token"

    private fun createTestConfig(): ServerConfiguration {
        val resolver = LinuxToolResolver(toolsBinDir = File("."))
        val runner = LinuxProcessRunner(resolver)
        return ServerConfiguration(
            authToken = testToken,
            distroName = "Ubuntu-Test",
            toolResolver = resolver,
            processRunner = runner
        )
    }

    private fun ApplicationTestBuilder.setupTestApp(config: ServerConfiguration) {
        application {
            install(ServerContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
            install(io.ktor.server.websocket.WebSockets)
            configureRoutes(config)
        }
    }

    @Test
    fun `health endpoint returns unauthenticated server info`() = testApplication {
        val config = createTestConfig()
        setupTestApp(config)

        val client = createClient {
            install(ClientContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
        }

        val response = client.get("/api/v1/health")
        assertEquals(HttpStatusCode.OK, response.status)
        val info = Json.decodeFromString<WslServerInfo>(response.bodyAsText())
        assertEquals("UP", info.status)
        assertEquals("Ubuntu-Test", info.distro)
    }

    @Test
    fun `authenticated routes reject unauthenticated requests`() = testApplication {
        val config = createTestConfig()
        setupTestApp(config)

        val response = client.get("/api/v1/tools")
        assertEquals(HttpStatusCode.Unauthorized, response.status)
    }

    @Test
    fun `authenticated routes succeed with valid Bearer token`() = testApplication {
        val config = createTestConfig()
        setupTestApp(config)

        val response = client.get("/api/v1/tools") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.OK, response.status)
    }

    @Test
    fun `execute returns response for tool execution request`() = testApplication {
        val config = createTestConfig()
        setupTestApp(config)

        val client = createClient {
            install(ClientContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
        }

        val response = client.post("/api/v1/tools/execute") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            contentType(ContentType.Application.Json)
            setBody(ToolExecutionRequest(toolId = "mke2fs", arguments = listOf("-V")))
        }
        assertEquals(HttpStatusCode.OK, response.status)
        assertEquals("1.2.0", response.headers["X-LtiRom-Protocol-Version"])
    }

    @Test
    fun `server accepts 1_1_0 and 1_2_0 and rejects unsupported protocol versions`() = testApplication {
        val config = createTestConfig()
        setupTestApp(config)

        val client = createClient {
            install(ClientContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
        }

        val res110 = client.get("/api/v1/health") {
            header("X-LtiRom-Protocol-Version", "1.1.0")
        }
        assertEquals(HttpStatusCode.OK, res110.status)
        assertEquals("1.2.0", res110.headers["X-LtiRom-Protocol-Version"])

        val res120 = client.get("/api/v1/health") {
            header("X-LtiRom-Protocol-Version", "1.2.0")
        }
        assertEquals(HttpStatusCode.OK, res120.status)
        assertEquals("1.2.0", res120.headers["X-LtiRom-Protocol-Version"])

        val resInvalid = client.get("/api/v1/health") {
            header("X-LtiRom-Protocol-Version", "0.9.0")
        }
        assertEquals(HttpStatusCode.BadRequest, resInvalid.status)
    }

    @Test
    fun `cancel endpoint dispatches cancellation for running process`() = testApplication {
        val config = createTestConfig()
        setupTestApp(config)

        val client = createClient {
            install(ClientContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
        }

        val response = client.post("/api/v1/tools/execute/fake-exec-id-1/cancel") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.OK, response.status)
        val body = Json.decodeFromString<ProcessCancellationResponse>(response.bodyAsText())
        assertEquals("fake-exec-id-1", body.executionId)
    }

    @Test
    fun `tools refresh endpoint requires authentication`() = testApplication {
        val config = createTestConfig()
        setupTestApp(config)

        val response = client.post("/api/v1/tools/refresh")
        assertEquals(HttpStatusCode.Unauthorized, response.status)
    }

    @Test
    fun `tools refresh endpoint rescans tools and returns tool status list`() = testApplication {
        val tempDir = java.nio.file.Files.createTempDirectory("ltirom_test_tools_d").toFile()
        tempDir.deleteOnExit()
        val resolver = LinuxToolResolver(toolsBinDir = File("."))
        val dynamicRegistry = io.ltirom.server.infra.DynamicToolRegistry(
            fallbackResolver = resolver,
            pluginDir = tempDir
        )
        val runner = LinuxProcessRunner(dynamicRegistry)
        val config = ServerConfiguration(
            authToken = testToken,
            distroName = "Ubuntu-Test",
            toolResolver = dynamicRegistry,
            processRunner = runner
        )
        setupTestApp(config)

        val client = createClient {
            install(ClientContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
        }

        val initialResp = client.post("/api/v1/tools/refresh") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.OK, initialResp.status)

        // Drop a new tool manifest
        val dummyBin = File(tempDir, "dummy_bin")
        dummyBin.writeText("#!/bin/sh\necho hi")
        dummyBin.setExecutable(true)
        val manifest = File(tempDir, "dummy.json")
        manifest.writeText("""{"name":"dummy","executable":"${dummyBin.absolutePath.replace("\\", "/")}"}""")

        val refreshResp = client.post("/api/v1/tools/refresh") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.OK, refreshResp.status)
        val tools = Json.decodeFromString<List<ToolStatusInfo>>(refreshResp.bodyAsText())
        val dummy = tools.find { it.tool == "dummy" }
        assertNotNull(dummy)
        assertTrue(dummy.installed)
    }

    @Test
    fun `binary endpoints enforce path containment for upload and download`() = testApplication {
        val tempRoot = java.nio.file.Files.createTempDirectory("lti_containment_test").toFile()
        tempRoot.deleteOnExit()
        val allowedWorkDir = File(tempRoot, "LtiRomWorkDir").apply { mkdirs() }
        val allowedLtiRom = File(tempRoot, ".ltirom").apply { mkdirs() }
        val outsideDir = File(tempRoot, "outside").apply { mkdirs() }

        val config = ServerConfiguration(
            authToken = testToken,
            distroName = "Ubuntu-Test",
            toolResolver = LinuxToolResolver(toolsBinDir = File(".")),
            processRunner = LinuxProcessRunner(LinuxToolResolver(toolsBinDir = File("."))),
            allowedBaseDirs = listOf(allowedWorkDir, allowedLtiRom)
        )
        setupTestApp(config)

        val client = createClient {
            install(ClientContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
        }

        // 1. Upload to absolute path elsewhere -> 403 PathNotAllowed
        val outsideUpload = File(outsideDir, "upload.bin")
        val res1 = client.post("/api/v1/binary/upload?path=${outsideUpload.absolutePath.replace("\\", "/")}") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            setBody("forbidden content".toByteArray())
        }
        assertEquals(HttpStatusCode.Forbidden, res1.status)
        assertEquals("PathNotAllowed", res1.bodyAsText())

        // 2. Upload with .. escape -> 403 PathNotAllowed
        val dotDotUploadPath = "${allowedWorkDir.absolutePath.replace("\\", "/")}/../outside/escape.bin"
        val res2 = client.post("/api/v1/binary/upload?path=$dotDotUploadPath") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            setBody("escape content".toByteArray())
        }
        assertEquals(HttpStatusCode.Forbidden, res2.status)
        assertEquals("PathNotAllowed", res2.bodyAsText())

        // 3. Upload through symlink escape -> 403 PathNotAllowed (if symlinks supported)
        val symlinkDir = File(allowedWorkDir, "symlink_outside")
        val symlinkCreated = runCatching {
            java.nio.file.Files.createSymbolicLink(symlinkDir.toPath(), outsideDir.toPath())
        }.isSuccess
        if (symlinkCreated) {
            val symlinkUploadPath = "${symlinkDir.absolutePath.replace("\\", "/")}/symlink_escape.bin"
            val resSym = client.post("/api/v1/binary/upload?path=$symlinkUploadPath") {
                header(HttpHeaders.Authorization, "Bearer $testToken")
                setBody("symlink escape content".toByteArray())
            }
            assertEquals(HttpStatusCode.Forbidden, resSym.status)
            assertEquals("PathNotAllowed", resSym.bodyAsText())
        }

        // 4. Upload to allowed directory -> 200 OK
        val validUpload = File(allowedWorkDir, "valid.bin")
        val resValid = client.post("/api/v1/binary/upload?path=${validUpload.absolutePath.replace("\\", "/")}") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
            setBody("valid content".toByteArray())
        }
        assertEquals(HttpStatusCode.OK, resValid.status)
        assertTrue(validUpload.exists())
        assertEquals("valid content", validUpload.readText())

        // 5. Download from absolute path elsewhere -> 403 PathNotAllowed
        val outsideFile = File(outsideDir, "outside_download.bin").apply { writeText("secret") }
        val resDown1 = client.get("/api/v1/binary/download?path=${outsideFile.absolutePath.replace("\\", "/")}") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.Forbidden, resDown1.status)
        assertEquals("PathNotAllowed", resDown1.bodyAsText())

        // 6. Download with .. escape -> 403 PathNotAllowed
        val dotDotDownloadPath = "${allowedWorkDir.absolutePath.replace("\\", "/")}/../outside/outside_download.bin"
        val resDown2 = client.get("/api/v1/binary/download?path=$dotDotDownloadPath") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.Forbidden, resDown2.status)
        assertEquals("PathNotAllowed", resDown2.bodyAsText())

        // 7. Download through symlink escape -> 403 PathNotAllowed
        if (symlinkCreated) {
            val symlinkDownloadPath = "${symlinkDir.absolutePath.replace("\\", "/")}/outside_download.bin"
            val resDownSym = client.get("/api/v1/binary/download?path=$symlinkDownloadPath") {
                header(HttpHeaders.Authorization, "Bearer $testToken")
            }
            assertEquals(HttpStatusCode.Forbidden, resDownSym.status)
            assertEquals("PathNotAllowed", resDownSym.bodyAsText())
        }

        // 8. Download from allowed directory -> 200 OK
        val resDownValid = client.get("/api/v1/binary/download?path=${validUpload.absolutePath.replace("\\", "/")}") {
            header(HttpHeaders.Authorization, "Bearer $testToken")
        }
        assertEquals(HttpStatusCode.OK, resDownValid.status)
        assertEquals("valid content", resDownValid.bodyAsText())
    }
}


