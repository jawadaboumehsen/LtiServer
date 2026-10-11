package io.ltirom.server.engine

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class ClientWatchdogTest {

    @Test
    fun `no heartbeat yet - admission is closed`() {
        var fakeTime = 1000L
        val watchdog = ClientWatchdog(
            clock = { fakeTime },
            heartbeatTimeoutMs = 30_000L,
        )
        assertFalse(watchdog.isAdmissionOpen, "Admission must be closed before first heartbeat")
    }

    @Test
    fun `fresh heartbeat opens admission and maintains open state while fresh`() {
        var fakeTime = 1000L
        val watchdog = ClientWatchdog(
            clock = { fakeTime },
            heartbeatTimeoutMs = 30_000L,
        )
        val accepted = watchdog.recordHeartbeat()
        assertTrue(accepted, "First heartbeat must be accepted")
        assertTrue(watchdog.isAdmissionOpen, "Admission must open after fresh heartbeat")

        fakeTime += 15_000L
        assertTrue(watchdog.isAdmissionOpen, "Admission must remain open at 15s")
    }

    @Test
    fun `heartbeat 31s ago with active runs triggers cancellation`() = runTest {
        var fakeTime = 1000L
        val cancelledWork = mutableListOf<String>()
        val activeWork = mutableListOf("run-1", "exec-2")

        val watchdog = ClientWatchdog(
            clock = { fakeTime },
            heartbeatTimeoutMs = 30_000L,
            cancelAllWork = {
                cancelledWork.addAll(activeWork)
                activeWork.clear()
                cancelledWork.toList()
            },
            activeWorkProvider = { activeWork.toList() },
            coroutineScope = this,
        )

        watchdog.recordHeartbeat()
        assertTrue(watchdog.isAdmissionOpen)

        // Advance fake clock and coroutine scheduler by 31s
        fakeTime += 31_000L
        advanceTimeBy(31_000L)

        assertFalse(watchdog.isAdmissionOpen, "Admission must close after 30s timeout")
        assertEquals(listOf("run-1", "exec-2"), cancelledWork, "Active work must be cancelled on deadline")
    }

    @Test
    fun `client stop closes admission and cancels executions`() = runTest {
        var fakeTime = 1000L
        val activeWork = mutableListOf("run-active")

        val watchdog = ClientWatchdog(
            clock = { fakeTime },
            heartbeatTimeoutMs = 30_000L,
            stopWaitTimeoutMs = 2_000L,
            cancelAllWork = {
                val list = activeWork.toList()
                activeWork.clear()
                list
            },
            activeWorkProvider = { activeWork.toList() },
            coroutineScope = this,
        )

        watchdog.recordHeartbeat()
        assertTrue(watchdog.isAdmissionOpen)

        val result = watchdog.stop(timeoutMs = 1_000L)
        assertTrue(result.stopped)
        assertTrue(result.unconfirmedExecutionIds.isEmpty())
        assertFalse(watchdog.isAdmissionOpen, "Admission must remain closed after stop")

        // A persistent daemon accepts the next authenticated desktop client
        // once cancellation is complete.
        val heartbeatAfterStop = watchdog.recordHeartbeat()
        assertTrue(heartbeatAfterStop, "A fresh client must reopen admission after a clean stop")
        watchdog.close()
    }

    @Test
    fun `heartbeat during unconfirmed cancellation cannot reopen admission`() = runTest {
        var fakeTime = 1000L
        val activeWork = mutableListOf("stuck-run")

        val watchdog = ClientWatchdog(
            clock = { fakeTime },
            heartbeatTimeoutMs = 30_000L,
            stopWaitTimeoutMs = 500L,
            cancelAllWork = {
                // Return work that cannot be confirmed stopped
                activeWork.toList()
            },
            activeWorkProvider = { activeWork.toList() },
            coroutineScope = this,
        )

        watchdog.recordHeartbeat()
        fakeTime += 31_000L
        advanceTimeBy(31_000L)

        // Stale work is unconfirmed
        assertFalse(watchdog.isAdmissionOpen)

        // Heartbeat should not reopen admission while unconfirmed work exists
        val heartbeatResult = watchdog.recordHeartbeat()
        assertFalse(heartbeatResult, "Heartbeat must be rejected while work remains unconfirmed")
        assertFalse(watchdog.isAdmissionOpen)
    }

    @Test
    fun `admissionState transitions correctly across lifecycle`() = runTest {
        var fakeTime = 1000L
        val watchdog = ClientWatchdog(
            clock = { fakeTime },
            heartbeatTimeoutMs = 30_000L,
            stopWaitTimeoutMs = 1_000L,
            coroutineScope = this,
        )

        assertEquals(WatchdogAdmissionState.AWAITING_HEARTBEAT, watchdog.admissionState)

        assertTrue(watchdog.recordHeartbeat())
        assertEquals(WatchdogAdmissionState.ADMISSION_OPEN, watchdog.admissionState)

        val stopResult = watchdog.stop()
        assertTrue(stopResult.stopped)
        assertEquals(WatchdogAdmissionState.STOPPED, watchdog.admissionState)

        // Fresh session reopens admission
        assertTrue(watchdog.recordHeartbeat())
        assertEquals(WatchdogAdmissionState.ADMISSION_OPEN, watchdog.admissionState)
        watchdog.close()
    }

    @Test
    fun `late heartbeat during active stopping returns false and cannot undo stopping`() = runTest {
        var fakeTime = 1000L
        val activeWork = mutableListOf("work-in-progress")
        val watchdog = ClientWatchdog(
            clock = { fakeTime },
            heartbeatTimeoutMs = 30_000L,
            stopWaitTimeoutMs = 2_000L,
            cancelAllWork = {
                activeWork.toList()
            },
            activeWorkProvider = { activeWork.toList() },
            coroutineScope = this,
        )

        assertTrue(watchdog.recordHeartbeat())
        assertTrue(watchdog.isAdmissionOpen)

        val stopJob = launch { watchdog.stop() }
        kotlinx.coroutines.delay(50L)

        assertFalse(watchdog.recordHeartbeat(), "Late heartbeat during stopping must be rejected")

        activeWork.clear()
        stopJob.join()

        assertEquals(WatchdogAdmissionState.STOPPED, watchdog.admissionState)
    }

    @Test
    fun `stale deadline callback does not transition state if superseded by newer heartbeat`() = runTest {
        var fakeTime = 1000L
        val watchdog = ClientWatchdog(
            clock = { fakeTime },
            heartbeatTimeoutMs = 30_000L,
            stopWaitTimeoutMs = 1_000L,
            coroutineScope = this,
        )

        assertTrue(watchdog.recordHeartbeat())

        fakeTime = 20_000L
        assertTrue(watchdog.recordHeartbeat())

        fakeTime = 32_000L
        advanceTimeBy(32_000L)

        assertTrue(watchdog.isAdmissionOpen, "Admission must remain open when deadline was rearmed")
        assertEquals(WatchdogAdmissionState.ADMISSION_OPEN, watchdog.admissionState)
        watchdog.close()
    }

    @Test
    fun `concurrent overlapping stops are serialized and do not corrupt generation or state`() = runTest {
        var fakeTime = 1000L
        val activeWork = mutableListOf("run-1")

        val watchdog = ClientWatchdog(
            clock = { fakeTime },
            heartbeatTimeoutMs = 30_000L,
            stopWaitTimeoutMs = 500L,
            cancelAllWork = {
                delay(50)
                activeWork.clear()
                listOf("run-1")
            },
            activeWorkProvider = { activeWork.toList() },
            coroutineScope = this,
        )

        assertTrue(watchdog.recordHeartbeat())
        val stop1 = launch { watchdog.stop() }
        val stop2 = launch { watchdog.stop() }

        stop1.join()
        stop2.join()

        assertEquals(WatchdogAdmissionState.STOPPED, watchdog.admissionState)
        assertFalse(watchdog.isAdmissionOpen)
    }

    @Test
    fun `work that is still running after the deadline blocks admission even under another label`() = runTest {
        var fakeTime = 1000L
        // cancelAllWork lists the streaming command as "stream:cmd-1"; it then shows up as "execute:cmd-1".
        var active = listOf("stream:cmd-1")
        val watchdog = ClientWatchdog(
            clock = { fakeTime },
            heartbeatTimeoutMs = 30_000L,
            cancelAllWork = {
                val cancelled = active
                active = listOf("execute:cmd-1")
                cancelled
            },
            activeWorkProvider = { active },
            coroutineScope = this,
        )
        watchdog.recordHeartbeat()

        fakeTime += 31_000L
        advanceTimeBy(31_000L)
        advanceTimeBy(1)

        assertFalse(watchdog.recordHeartbeat(), "something is still running: no new admission yet")

        active = emptyList()
        assertTrue(watchdog.recordHeartbeat(), "admission reopens once nothing is left")
        watchdog.close()
    }
}
