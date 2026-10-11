package io.ltirom.server.routing

import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import io.ktor.serialization.kotlinx.json.json
import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.infra.planUpload
import io.ltirom.server.infra.writeReplacing
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RoutingSecurityTest {
    private val token = "secret-token"
    private lateinit var root: File
    private lateinit var allowed: File

    @BeforeTest
    fun setUp() {
        root = Files.createTempDirectory("routing-security").toFile()
        allowed = File(root, "allowed").also { it.mkdirs() }
    }

    @AfterTest
    fun tearDown() {
        root.deleteRecursively()
    }

    private class Resolver : ToolResolverPort {
        override fun resolve(toolName: String): File? = null

        override fun listAvailableTools(): Map<String, Boolean> = emptyMap()
    }

    private class Runner : ProcessExecutionPort {
        override suspend fun runProcess(
            toolName: String,
            arguments: List<String>,
            workingDir: File,
            environment: Map<String, String>,
            stdinText: String?,
            timeoutMs: Long,
            onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?,
            executionId: String?,
        ): ProcessExecutionResult = ProcessExecutionResult(0, "", "", 1L)
    }

    private fun ApplicationTestBuilder.app() {
        val config =
            ServerConfiguration(
                authToken = token,
                distroName = "Ubuntu-Test",
                toolResolver = Resolver(),
                processRunner = Runner(),
                allowedBaseDirs = listOf(allowed),
                exitOnShutdown = false,
            )
        application {
            install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
            install(io.ktor.server.websocket.WebSockets)
            configureRoutes(config)
        }
    }

    // ------------------------------------------------------------------ authentication

    @Test
    fun `a protected endpoint is not reachable by putting health in the query string`() = testApplication {
        app()

        for (suffix in listOf("?x=/health", "?next=/health", "?a=b&c=/health")) {
            assertEquals(HttpStatusCode.Unauthorized, client.get("/api/v1/tools$suffix").status, suffix)
            assertEquals(
                HttpStatusCode.Unauthorized,
                client.post("/api/v1/client/heartbeat$suffix").status,
                "heartbeat $suffix",
            )
            assertEquals(
                HttpStatusCode.Unauthorized,
                client.get("/api/v1/tools$suffix") { header(HttpHeaders.Authorization, "Bearer wrong") }.status,
            )
        }
        assertEquals(
            HttpStatusCode.OK,
            client.get("/api/v1/tools?x=/health") { header(HttpHeaders.Authorization, "Bearer $token") }.status,
            "the right token still works",
        )
    }

    @Test
    fun `the health endpoint itself stays open`() = testApplication {
        app()

        assertEquals(HttpStatusCode.OK, client.get("/api/v1/health").status)
    }

    @Test
    fun `an upload with a health query string and no token is refused and writes nothing`() = testApplication {
        app()
        val target = File(allowed, "x.bin")

        val response = client.post("/api/v1/binary/upload?path=${target.absolutePath}&x=/health") { setBody("data") }

        assertEquals(HttpStatusCode.Unauthorized, response.status)
        assertFalse(target.exists())
    }

    // ------------------------------------------------------------------ uploads

    private val roots: List<Path> get() = listOf(allowed.toPath().toRealPath())

    @Test
    fun `a refused upload creates nothing, an allowed one needs no directory to exist yet`() {
        val outside = File(root, "outside/deep/file.bin")
        assertNull(planUpload(outside, roots))
        assertFalse(File(root, "outside").exists(), "a refused request must not create directories")

        val nested = File(allowed, "a/b/c/file.bin")
        val plan = assertNotNull(planUpload(nested, roots))
        assertTrue(plan.startsWith(roots.single()))
        assertFalse(File(allowed, "a").exists(), "planning alone creates nothing either")

        assertNull(planUpload(File(allowed, "../outside.bin"), roots), "dot-dot out of the root")
        assertNull(planUpload(allowed, roots), "a directory is not a file target")
    }

    @Test
    fun `a failed upload leaves the previous file exactly as it was`() = runBlocking<Unit> {
        val target = File(allowed, "config.json").also { it.writeText("previous") }

        assertFailsWith<java.io.IOException> {
            writeReplacing(target.toPath()) { out ->
                out.write("half of the new con".toByteArray())
                throw java.io.IOException("connection reset")
            }
        }

        assertEquals("previous", target.readText())
        assertEquals(listOf("config.json"), allowed.list()!!.toList(), "no partial file is left behind")
    }

    @Test
    fun `a good upload replaces the file whole and creates missing directories`() = runBlocking<Unit> {
        val target = File(allowed, "new/dir/tool.bin")

        val written = writeReplacing(target.toPath()) { out ->
            out.write("complete".toByteArray())
            8L
        }

        assertEquals(8L, written)
        assertEquals("complete", target.readText())
        assertEquals(listOf("tool.bin"), target.parentFile.list()!!.toList())
    }

    private fun symlinkOrSkip(link: Path, target: Path) {
        val created = runCatching { Files.createSymbolicLink(link, target) }.isSuccess
        org.junit.Assume.assumeTrue("symbolic links are not available here", created)
    }

    @Test
    fun `a symlink at the destination is replaced and its target is left untouched`() = runBlocking<Unit> {
        val victim = File(root, "outside-victim.txt").also { it.writeText("do not touch") }
        val link = File(allowed, "innocent.bin")
        symlinkOrSkip(link.toPath(), victim.toPath())

        val plan = assertNotNull(planUpload(link, roots))
        writeReplacing(plan) { out ->
            out.write("new content".toByteArray())
            11L
        }

        assertEquals("do not touch", victim.readText(), "the file the link pointed to must not be overwritten")
        assertEquals("new content", link.readText())
        assertFalse(Files.isSymbolicLink(link.toPath()), "the link itself was replaced by the uploaded file")
    }

    // The planned parent is replaced by a symlink between planUpload and the write: nothing may reach the outside.
    @Test
    fun `an ancestor replaced by a symlink after the plan cannot redirect the write`() = runBlocking<Unit> {
        val outsideDir = File(root, "outside-race").also { it.mkdirs() }
        val sub = File(allowed, "sub").also { it.mkdirs() }
        val plan = assertNotNull(planUpload(File(sub, "tool.bin"), roots))

        sub.deleteRecursively()
        symlinkOrSkip(sub.toPath(), outsideDir.toPath())

        assertFailsWith<java.io.IOException> {
            writeReplacing(plan) { out ->
                out.write("payload".toByteArray())
                7L
            }
        }
        assertEquals(emptyList(), outsideDir.list()!!.toList(), "no file, not even a temporary one, may land outside")
    }

    @Test
    fun `a symlinked directory cannot lead an upload out of the allowed roots`() {
        val outsideDir = File(root, "outside-dir").also { it.mkdirs() }
        symlinkOrSkip(File(allowed, "escape").toPath(), outsideDir.toPath())

        assertNull(planUpload(File(allowed, "escape/stolen.bin"), roots))
        assertNull(planUpload(File(allowed, "escape/new/deeper.bin"), roots))
    }
}
