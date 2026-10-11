package io.ltirom.server.session

import io.ltirom.server.model.StreamEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

/** A slow reader used to lose events (and with them ExecutionFinished, so the client waited for the time limit). */
class AttachDeliversEverythingTest {

    private fun session() = ExecutionSession(sessionId = "s", toolId = "sh", arguments = emptyList(), initialSeq = 1L)

    @Test
    fun `a reader far behind still gets every event and the finish`() = runBlocking<Unit> {
        val session = session()
        val firstSeen = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val reader = async(Dispatchers.Default) {
            withTimeout(10_000) {
                val got = mutableListOf<Long>()
                session.attach(1L).collect {
                    got += it.seq
                    if (got.size == 1) {
                        firstSeen.complete(Unit)
                        release.await() // a WebSocket that can't keep up
                    }
                }
                got
            }
        }
        session.appendEvent(StreamEvent.OutputChunk("line 0"))
        firstSeen.await()
        repeat(1500) { session.appendEvent(StreamEvent.OutputChunk("line ${it + 1}")) }
        session.appendEvent(StreamEvent.ExecutionFinished(0, 1L))
        release.complete(Unit)

        assertEquals((1L..1502L).toList(), reader.await())
    }

    @Test
    fun `attaching before any output ends with the finish`() = runBlocking<Unit> {
        val session = session()
        val reader = async(Dispatchers.Default) { withTimeout(10_000) { session.attach(1L).toList() } }
        Thread.sleep(50)
        session.appendEvent(StreamEvent.ExecutionFinished(3, 1L))

        assertIs<StreamEvent.ExecutionFinished>(reader.await().single().event)
    }
}
