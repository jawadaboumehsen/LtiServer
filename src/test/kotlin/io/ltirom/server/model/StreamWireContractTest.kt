package io.ltirom.server.model

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * The stream's type names are a wire contract: the desktop client decodes exactly these names, and run journals
 * on disk hold them. Renaming or moving StreamEvent changes them and silently breaks every client.
 */
class StreamWireContractTest {
    private val json = Json

    @Test
    fun `stream events keep their wire names`() {
        val expected = mapOf(
            StreamEvent.OutputChunk("x", isError = true) to "io.ltirom.server.model.StreamEvent.OutputChunk",
            StreamEvent.ProgressUpdate(0.5f) to "io.ltirom.server.model.StreamEvent.ProgressUpdate",
            StreamEvent.ExecutionFinished(0, 1) to "io.ltirom.server.model.StreamEvent.ExecutionFinished",
            StreamEvent.Heartbeat(1) to "io.ltirom.server.model.StreamEvent.Heartbeat",
        )
        for ((event, name) in expected) {
            val wire = json.encodeToString(SequencedStreamEvent(1, event))
            assertTrue("\"type\":\"$name\"" in wire, wire)
        }
    }
}
