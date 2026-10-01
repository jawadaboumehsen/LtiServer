package io.ltirom.server.model

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.test.*

class RunProtocolTest {

    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
        encodeDefaults = true
    }

    @Test
    fun testStartRunRequestWireFormat() {
        val payload = """
            {
                "request": {
                    "toolId": "mkfs.erofs",
                    "arguments": ["-z", "lz4hc", "out.img", "work/"],
                    "workingDirectory": "/home/lti/LtiRomWorkDir/workspaces/ws1",
                    "environment": {},
                    "timeoutMs": 0,
                    "acceptedExitCodes": [0],
                    "streamOutput": true
                },
                "idempotencyKey": "run-1:FIRMWARE_EXTRACTION:0",
                "workspaceLock": "/home/lti/LtiRomWorkDir/workspaces/ws1"
            }
        """.trimIndent()

        val decoded = json.decodeFromString<StartRunRequest>(payload)
        assertEquals("mkfs.erofs", decoded.request.toolId)
        assertEquals(listOf("-z", "lz4hc", "out.img", "work/"), decoded.request.arguments)
        assertEquals("/home/lti/LtiRomWorkDir/workspaces/ws1", decoded.request.workingDirectory)
        assertEquals("run-1:FIRMWARE_EXTRACTION:0", decoded.idempotencyKey)
        assertEquals("/home/lti/LtiRomWorkDir/workspaces/ws1", decoded.workspaceLock)

        val serialized = json.encodeToString(decoded)
        val roundtrip = json.decodeFromString<StartRunRequest>(serialized)
        assertEquals(decoded, roundtrip)
    }

    @Test
    fun testRunHandleWireFormat() {
        val payload = """
            {
                "runId": "run-42",
                "status": "RUNNING",
                "startedAtEpochMs": 1726050000000
            }
        """.trimIndent()

        val decoded = json.decodeFromString<RunHandle>(payload)
        assertEquals("run-42", decoded.runId)
        assertEquals(RunStatusValue.RUNNING, decoded.status)
        assertEquals(1726050000000L, decoded.startedAtEpochMs)

        val serialized = json.encodeToString(decoded)
        val roundtrip = json.decodeFromString<RunHandle>(serialized)
        assertEquals(decoded, roundtrip)
    }

    @Test
    fun testRunConflictWireFormat() {
        val payload = """
            {
                "existingRunId": "run-existing-1",
                "message": "A run is already active in workspace"
            }
        """.trimIndent()

        val decoded = json.decodeFromString<RunConflict>(payload)
        assertEquals("run-existing-1", decoded.existingRunId)
        assertEquals("A run is already active in workspace", decoded.message)

        val serialized = json.encodeToString(decoded)
        val roundtrip = json.decodeFromString<RunConflict>(serialized)
        assertEquals(decoded, roundtrip)
    }

    @Test
    fun testInvalidWorkingDirectoryWireFormat() {
        val payload = """
            {
                "path": "/invalid/path",
                "reason": "working directory missing"
            }
        """.trimIndent()

        val decoded = json.decodeFromString<InvalidWorkingDirectory>(payload)
        assertEquals("/invalid/path", decoded.path)
        assertEquals("working directory missing", decoded.reason)

        val serialized = json.encodeToString(decoded)
        val roundtrip = json.decodeFromString<InvalidWorkingDirectory>(serialized)
        assertEquals(decoded, roundtrip)
    }

    @Test
    fun testRunStatusWireFormat() {
        val payload = """
            {
                "runId": "run-99",
                "status": "COMPLETED",
                "exitCode": 0,
                "pid": 12345,
                "startedAtEpochMs": 1726050000000,
                "endedAtEpochMs": 1726050015000,
                "lastSeq": 4821
            }
        """.trimIndent()

        val decoded = json.decodeFromString<RunStatus>(payload)
        assertEquals("run-99", decoded.runId)
        assertEquals(RunStatusValue.COMPLETED, decoded.status)
        assertEquals(0, decoded.exitCode)
        assertEquals(12345L, decoded.pid)
        assertEquals(1726050000000L, decoded.startedAtEpochMs)
        assertEquals(1726050015000L, decoded.endedAtEpochMs)
        assertEquals(4821L, decoded.lastSeq)

        val serialized = json.encodeToString(decoded)
        val roundtrip = json.decodeFromString<RunStatus>(serialized)
        assertEquals(decoded, roundtrip)

        // Test minimal / nullable fields
        val minimalPayload = """
            {
                "runId": "run-100",
                "status": "QUEUED",
                "startedAtEpochMs": 1726050020000
            }
        """.trimIndent()
        val minimalDecoded = json.decodeFromString<RunStatus>(minimalPayload)
        assertEquals("run-100", minimalDecoded.runId)
        assertEquals(RunStatusValue.QUEUED, minimalDecoded.status)
        assertNull(minimalDecoded.exitCode)
        assertNull(minimalDecoded.pid)
        assertNull(minimalDecoded.endedAtEpochMs)
        assertEquals(0L, minimalDecoded.lastSeq)
    }

    @Test
    fun testRunCancelResponseWireFormat() {
        val payload = """
            {
                "accepted": true,
                "status": "CANCELLING",
                "message": "SIGTERM sent"
            }
        """.trimIndent()

        val decoded = json.decodeFromString<RunCancelResponse>(payload)
        assertTrue(decoded.accepted)
        assertEquals(RunStatusValue.CANCELLING, decoded.status)
        assertEquals("SIGTERM sent", decoded.message)

        val serialized = json.encodeToString(decoded)
        val roundtrip = json.decodeFromString<RunCancelResponse>(serialized)
        assertEquals(decoded, roundtrip)
    }

    @Test
    fun testActiveRunsWireFormat() {
        val payload = """
            {
                "runIds": ["run-1", "run-2"],
                "message": "Active runs prevent shutdown"
            }
        """.trimIndent()

        val decoded = json.decodeFromString<ActiveRuns>(payload)
        assertEquals(listOf("run-1", "run-2"), decoded.runIds)
        assertEquals("Active runs prevent shutdown", decoded.message)

        val serialized = json.encodeToString(decoded)
        val roundtrip = json.decodeFromString<ActiveRuns>(serialized)
        assertEquals(decoded, roundtrip)
    }

    @Test
    fun testRunStatusValueEnumCoverage() {
        for (value in RunStatusValue.entries) {
            val serialized = json.encodeToString(value)
            val decoded = json.decodeFromString<RunStatusValue>(serialized)
            assertEquals(value, decoded)
        }
    }
}
