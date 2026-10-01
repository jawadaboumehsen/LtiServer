package io.ltirom.server.session

import io.ltirom.server.model.SessionStatus
import io.ltirom.server.model.StreamEvent
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class ExecutionSessionManagerTest {

    @Test
    fun `circular buffer preserves events and assigns monotonic sequence numbers`() {
        val session = ExecutionSession(
            sessionId = "test-session-1",
            toolId = "mke2fs",
            arguments = listOf("-t", "ext4"),
            bufferCapacity = 3
        )

        val e0 = session.appendEvent(StreamEvent.OutputChunk("line 0"))
        val e1 = session.appendEvent(StreamEvent.OutputChunk("line 1"))
        val e2 = session.appendEvent(StreamEvent.OutputChunk("line 2"))

        assertEquals(0L, e0.seq)
        assertEquals(1L, e1.seq)
        assertEquals(2L, e2.seq)

        // Replay from seq 1
        val replayFrom1 = session.ringBuffer.replay(fromSeq = 1L)
        assertEquals(2, replayFrom1.size)
        assertEquals(1L, replayFrom1[0].seq)
        assertEquals(2L, replayFrom1[1].seq)

        // Add 4th item to exceed capacity of 3
        val e3 = session.appendEvent(StreamEvent.OutputChunk("line 3"))
        assertEquals(3L, e3.seq)

        // All replay should now only have 3 items: seq 1, 2, 3 (seq 0 evicted)
        val allBuffered = session.ringBuffer.replay(fromSeq = 0L)
        assertEquals(3, allBuffered.size)
        assertEquals(1L, allBuffered.first().seq)
        assertEquals(3L, allBuffered.last().seq)
    }

    @Test
    fun `session transitions to completed when ExecutionFinished is appended`() {
        val manager = ExecutionSessionManager()
        val session = manager.createSession(toolId = "simg2img", arguments = listOf("a", "b"))

        assertEquals(SessionStatus.RUNNING, session.status.value)

        session.appendEvent(StreamEvent.ExecutionFinished(exitCode = 0, durationMs = 120L))
        assertEquals(SessionStatus.COMPLETED, session.status.value)
        assertEquals(0, session.exitCode)
        assertEquals(120L, session.durationMs)

        assertNotNull(manager.getSession(session.sessionId))
        assertEquals(1, manager.listSessions().size)
    }
}
