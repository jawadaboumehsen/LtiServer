package io.ltirom.server.session

import io.ltirom.server.infra.RunJournalStore
import io.ltirom.server.model.StreamEvent
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class EventOrderingAndReplayTest {
    private lateinit var dir: File

    @BeforeTest
    fun setUp() {
        dir = Files.createTempDirectory("event-order").toFile()
    }

    @AfterTest
    fun tearDown() {
        dir.deleteRecursively()
    }

    private fun session(capacity: Int = 10_000, journal: Boolean = true) = ExecutionSession(
        sessionId = "s1",
        toolId = "sh",
        arguments = emptyList(),
        bufferCapacity = capacity,
        journalStore = if (journal) RunJournalStore(dir, isPidAlive = { false }) else null,
        initialSeq = 1L,
    )

    private fun hammer(threads: Int, perThread: Int, action: (Int, Int) -> Unit) {
        val start = CountDownLatch(1)
        val workers = (0 until threads).map { t ->
            thread {
                start.await()
                repeat(perThread) { action(t, it) }
            }
        }
        start.countDown()
        workers.forEach { it.join() }
    }

    @Test
    fun `events from many threads are stored in the order they were numbered`() {
        val buffer = CircularEventBuffer(capacity = 100_000, initialSeq = 1L)

        hammer(threads = 8, perThread = 3_000) { t, i -> buffer.append(StreamEvent.OutputChunk("t$t-$i")) }

        val seqs = buffer.replay(0L).map { it.seq }
        assertEquals((1L..24_000L).toList(), seqs, "numbers are consecutive and in storage order")
    }

    @Test
    fun `a session numbers, stores and journals concurrent events in one order, and a reader gets every one`() =
        runBlocking<Unit> {
            val session = session()
            hammer(threads = 6, perThread = 500) { t, i -> session.appendEvent(StreamEvent.OutputChunk("t$t-$i")) }
            session.appendEvent(StreamEvent.ExecutionFinished(0, 1L))

            val journalOrder = RunJournalStore(dir, isPidAlive = { false }).replay("s1", 1L).map { it.seq }
            assertEquals((1L..3_001L).toList(), journalOrder, "the journal file is in sequence order too")

            val received = withTimeout(10_000) { session.attach(1L).toList() }
            assertEquals((1L..3_001L).toList(), received.map { it.seq })
            assertTrue(received.last().event is StreamEvent.ExecutionFinished)
        }

    @Test
    fun `the buffer is bounded by bytes as well as by count, and always keeps the newest event`() {
        val buffer = CircularEventBuffer(capacity = 10_000, initialSeq = 1L, maxBytes = 10_000)
        val bigLine = "x".repeat(2_000) // ~4 KB each

        repeat(50) { buffer.append(StreamEvent.OutputChunk(bigLine)) }

        val kept = buffer.replay(0L)
        assertTrue(kept.size in 1..3, "50 large events must not all be retained: ${kept.size}")
        assertEquals(50L, kept.last().seq, "the newest event is always kept")
        assertTrue(buffer.retainedBytes() <= 10_000 + 4_100, "retained ${buffer.retainedBytes()} bytes")

        val huge = CircularEventBuffer(capacity = 10, initialSeq = 1L, maxBytes = 100)
        huge.append(StreamEvent.OutputChunk("y".repeat(10_000)))
        assertEquals(1, huge.replay(0L).size, "a single oversized event is still kept")
    }

    @Test
    fun `history is read a page at a time and continues where the last page ended`() {
        val session = session(capacity = 50)
        repeat(1_200) { session.appendEvent(StreamEvent.OutputChunk("line $it")) }

        val first = session.replayPage(1L, 0L, limit = 500)
        assertEquals(500, first.events.size, "a page is bounded, however long the log")
        assertEquals(1L, first.events.first().seq)

        val second = session.replayPage(first.events.last().seq + 1, first.journalOffset, limit = 500)
        assertEquals(501L, second.events.first().seq, "the next page starts right after the first")
        assertTrue(second.journalOffset > first.journalOffset, "and does not rescan the file from the start")

        val all = runBlocking { session.replayFlow(1L).toList() }
        assertEquals((1L..1_200L).toList(), all.map { it.seq })
    }

    @Test
    fun `reattaching from an old cursor streams the log instead of loading it`() = runBlocking<Unit> {
        val session = session(capacity = 50)
        repeat(2_000) { session.appendEvent(StreamEvent.OutputChunk("line $it")) }
        session.appendEvent(StreamEvent.ExecutionFinished(0, 1L))

        val received = withTimeout(10_000) { session.attach(1L).toList() }

        assertEquals((1L..2_001L).toList(), received.map { it.seq })
    }

    @Test
    fun `a line still being written is not consumed or skipped`() {
        val session = session(capacity = 5)
        repeat(20) { session.appendEvent(StreamEvent.OutputChunk("line $it")) }
        File(dir, "s1/events.jsonl").appendText("{\"seq\":21,\"event\":") // a write in progress

        val page = session.replayPage(1L, 0L, limit = 1_000)

        assertEquals((1L..20L).toList(), page.events.map { it.seq })
        val end = File(dir, "s1/events.jsonl").length()
        assertTrue(page.journalOffset < end, "the offset stops before the unfinished line")
    }
}
