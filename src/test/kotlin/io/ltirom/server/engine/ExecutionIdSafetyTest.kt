package io.ltirom.server.engine

import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.infra.RunJournalStore
import io.ltirom.server.model.ToolExecutionRequest
import io.ltirom.server.session.ExecutionSessionManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Client-chosen ids become process keys and journal folders: they must be plain and unique among active work. */
class ExecutionIdSafetyTest {
    private lateinit var root: File
    private lateinit var journalDir: File
    private lateinit var engine: DefaultServerEngine

    private class Resolver : ToolResolverPort {
        override fun resolve(toolName: String): File? = File("/fake/$toolName")

        override fun listAvailableTools(): Map<String, Boolean> = mapOf("sleep" to true)
    }

    private class BlockingRunner : ProcessExecutionPort {
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
    }

    private val request = ToolExecutionRequest(toolId = "sleep", arguments = emptyList(), workingDirectory = File(".").absolutePath)

    @BeforeTest
    fun setUp() {
        root = Files.createTempDirectory("exec-ids").toFile()
        journalDir = File(root, "journal")
        engine =
            DefaultServerEngine(
                Resolver(),
                BlockingRunner(),
                "Ubuntu-Test",
                sessionManager = ExecutionSessionManager(journalStore = RunJournalStore(journalDir, isPidAlive = { false })),
            )
    }

    @AfterTest
    fun tearDown() {
        root.deleteRecursively()
    }

    private suspend fun awaitActive(count: Int) = withTimeout(5_000) {
        while (engine.getHealth().activeRuns != count) delay(10)
    }

    @Test
    fun `a second request with an id that is in use is refused and the first stays tracked`() = runBlocking<Unit> {
        val first = launch(Dispatchers.Default) { engine.execute(request, "same-id") }
        awaitActive(1)

        assertFailsWith<DuplicateExecutionException> { engine.execute(request, "same-id") }
        assertFailsWith<DuplicateExecutionException> { engine.stream(request, null, "same-id").toList() }

        assertEquals(listOf("execute:same-id"), engine.activeWork(), "the first execution is still tracked")
        assertEquals(listOf("execute:same-id"), engine.getHealth().activeWork, "the health report names what is tracked")
        first.cancel()
        first.join()
        awaitActive(0)
        val again = launch(Dispatchers.Default) { engine.execute(request, "same-id") }
        awaitActive(1) // free again once the first one has ended
        again.cancel()
        again.join()
    }

    @Test
    fun `ids that could leave the journal or collide with a path are refused before any file is touched`() =
        runBlocking<Unit> {
            for (bad in listOf("../escape", "..", "a/b", "a\\b", ".hidden", "", "x".repeat(200), "with space")) {
                assertFailsWith<InvalidExecutionIdException>("execute '$bad'") { engine.execute(request, bad) }
                assertFailsWith<InvalidExecutionIdException>("stream '$bad'") { engine.stream(request, null, bad).toList() }
            }

            assertFalse(File(root, "escape").exists())
            assertTrue(journalDir.listFiles().orEmpty().isEmpty(), "nothing was created in the journal")
        }

    @Test
    fun `the journal itself refuses a folder outside its root`() {
        val store = RunJournalStore(journalDir, isPidAlive = { false })

        assertFailsWith<IllegalArgumentException> { store.getRunDir("../escape") }
        assertFailsWith<IllegalArgumentException> { store.getRunDir("a/../../escape") }
        assertEquals(File(journalDir, "ok-1").canonicalFile, store.getRunDir("ok-1").canonicalFile)
    }

    @Test
    fun `uuids and the ids the app generates are accepted`() {
        listOf("0b8f2b0e-6a43-4c0c-9a1d-5f1f7f3a9c11", "exec-1a2b3c4d", "sess-1a2b3c4d", "trace-1").forEach {
            assertEquals(it, requireSafeExecutionId(it))
        }
    }

    @Test
    fun `an idle service reports no tracked work, so a vanished process can be told from a running one`() {
        assertEquals(emptyList(), engine.getHealth().activeWork)
    }
}
