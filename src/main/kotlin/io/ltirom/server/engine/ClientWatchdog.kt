package io.ltirom.server.engine

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable

@Serializable
public data class StopResult(
    val stopped: Boolean,
    val unconfirmedExecutionIds: List<String> = emptyList(),
)

public enum class WatchdogAdmissionState {
    AWAITING_HEARTBEAT,
    ADMISSION_OPEN,
    STOPPING,
    STOPPED,
    CANCELLING,
}

public class ClientWatchdog(
    // Monotonic: WSL's wall clock jumps (time sync, and by the whole sleep after the PC wakes), and a forward
    // jump of 30 s used to look like a vanished client and cancel every running build.
    private val clock: () -> Long = { System.nanoTime() / 1_000_000 },
    private val heartbeatTimeoutMs: Long = 30_000L,
    private val stopWaitTimeoutMs: Long = 10_000L,
    private val cancelAllWork: suspend () -> List<String> = { emptyList() },
    private val activeWorkProvider: () -> List<String> = { emptyList() },
    private val coroutineScope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob()),
    public var onAdmissionOpened: (() -> Unit)? = null,
) : AutoCloseable {
    private val lock = Any()
    private val stopMutex = kotlinx.coroutines.sync.Mutex()
    private var lastHeartbeatTime: Long? = null
    private var state: WatchdogAdmissionState = WatchdogAdmissionState.AWAITING_HEARTBEAT
    private var isCancelling: Boolean = false
    private var deadlineJob: Job? = null
    private var deadlineGeneration: Long = 0L
    private var unconfirmedIds: List<String> = emptyList()

    public val isAdmissionOpen: Boolean
        get() = synchronized(lock) {
            val last = lastHeartbeatTime
            state == WatchdogAdmissionState.ADMISSION_OPEN &&
                !isCancelling &&
                unconfirmedIds.isEmpty() &&
                last != null &&
                (clock() - last) <= heartbeatTimeoutMs
        }

    public val admissionState: WatchdogAdmissionState
        get() = synchronized(lock) {
            val last = lastHeartbeatTime
            if (state == WatchdogAdmissionState.ADMISSION_OPEN &&
                (last == null || (clock() - last) > heartbeatTimeoutMs)
            ) {
                WatchdogAdmissionState.AWAITING_HEARTBEAT
            } else {
                state
            }
        }

    public fun recordHeartbeat(): Boolean = synchronized(lock) {
        if (state == WatchdogAdmissionState.STOPPING || isCancelling) {
            return false
        }
        val active = activeWorkProvider()
        if (active.any { it in unconfirmedIds }) {
            return false
        }
        // A daemon can outlive one desktop client. Once all previous work is
        // gone, a fresh authenticated client may safely establish admission.
        unconfirmedIds = emptyList()
        val now = clock()
        lastHeartbeatTime = now
        val gen = ++deadlineGeneration
        val wasOpen = (state == WatchdogAdmissionState.ADMISSION_OPEN)
        state = WatchdogAdmissionState.ADMISSION_OPEN
        armDeadline(now, gen)
        if (!wasOpen) {
            onAdmissionOpened?.invoke()
        }
        return true
    }

    /**
     * A forced shutdown closed the engine's gate but [survivors] could not be stopped, so the service stays up.
     * The gate is reopened by the first heartbeat after they are gone: dropping to AWAITING_HEARTBEAT makes that
     * heartbeat an open transition, which heartbeats during the refusal (survivors still running) are not.
     */
    public fun shutdownRefused(survivors: List<String>): Unit = synchronized(lock) {
        unconfirmedIds = survivors
        deadlineGeneration++
        deadlineJob?.cancel()
        lastHeartbeatTime = null
        if (state == WatchdogAdmissionState.ADMISSION_OPEN) state = WatchdogAdmissionState.AWAITING_HEARTBEAT
    }

    private fun armDeadline(fromTime: Long, generation: Long) {
        deadlineJob?.cancel()
        deadlineJob = coroutineScope.launch {
            while (isActive) {
                val remaining = (fromTime + heartbeatTimeoutMs) - clock()
                if (remaining > 0) {
                    delay(remaining)
                } else {
                    break
                }
            }
            onDeadlineExpired(generation, fromTime)
        }
    }

    private suspend fun onDeadlineExpired(expectedGen: Long, expectedTime: Long) {
        var rearmTime: Long? = null
        val shouldCancel = synchronized(lock) {
            if (expectedGen != deadlineGeneration) {
                return
            }
            if (state != WatchdogAdmissionState.ADMISSION_OPEN) {
                return
            }
            val last = lastHeartbeatTime
            if (last == null) {
                return
            }
            if (last != expectedTime) {
                return
            }
            val elapsed = clock() - last
            if (elapsed < heartbeatTimeoutMs) {
                rearmTime = last
                return@synchronized false
            }
            isCancelling = true
            state = WatchdogAdmissionState.CANCELLING
            true
        }
        val t = rearmTime
        if (t != null) {
            armDeadline(t, expectedGen)
            return
        }
        if (!shouldCancel) return
        try {
            cancelAllWork()
            // Everything still active, not only what was on the cancel list: a command can be listed under
            // another label afterwards (stream:<id> / execute:<id>) and must not slip past the comparison.
            val remaining = activeWorkProvider()
            synchronized(lock) {
                if (expectedGen == deadlineGeneration) {
                    unconfirmedIds = remaining
                }
            }
        } finally {
            synchronized(lock) {
                if (expectedGen == deadlineGeneration) {
                    isCancelling = false
                    if (state != WatchdogAdmissionState.STOPPING &&
                        state != WatchdogAdmissionState.STOPPED
                    ) {
                        state = WatchdogAdmissionState.AWAITING_HEARTBEAT
                    }
                } else if (state == WatchdogAdmissionState.STOPPING || state == WatchdogAdmissionState.STOPPED) {
                    isCancelling = false
                }
            }
        }
    }

    public suspend fun stop(timeoutMs: Long = stopWaitTimeoutMs): StopResult = stopMutex.withLock {
        var earlyResult: StopResult? = null
        val stopGen = synchronized(lock) {
            if (state == WatchdogAdmissionState.STOPPED && activeWorkProvider().isEmpty()) {
                isCancelling = false
                earlyResult = StopResult(stopped = true, unconfirmedExecutionIds = emptyList())
                0L
            } else {
                state = WatchdogAdmissionState.STOPPING
                lastHeartbeatTime = null
                val gen = ++deadlineGeneration
                deadlineJob?.cancel()
                gen
            }
        }
        if (earlyResult != null) {
            return@withLock earlyResult!!
        }
        try {
            val startWait = clock()
            // Part of the same budget as the wait below, not in front of it.
            kotlinx.coroutines.withTimeoutOrNull(timeoutMs) { cancelAllWork() }
            while ((clock() - startWait) < timeoutMs) {
                val remaining = activeWorkProvider()
                if (remaining.isEmpty()) {
                    synchronized(lock) {
                        if (stopGen == deadlineGeneration) {
                            unconfirmedIds = emptyList()
                            isCancelling = false
                            state = WatchdogAdmissionState.STOPPED
                        }
                    }
                    return@withLock StopResult(stopped = true, unconfirmedExecutionIds = emptyList())
                }
                delay(50)
            }
            val unconfirmed = activeWorkProvider()
            synchronized(lock) {
                if (stopGen == deadlineGeneration) {
                    unconfirmedIds = unconfirmed
                    isCancelling = false
                    state = WatchdogAdmissionState.STOPPED
                }
            }
            StopResult(stopped = unconfirmed.isEmpty(), unconfirmedExecutionIds = unconfirmed)
        } finally {
            synchronized(lock) {
                if (stopGen == deadlineGeneration) {
                    isCancelling = false
                    if (state == WatchdogAdmissionState.STOPPING) {
                        unconfirmedIds = activeWorkProvider()
                        state = WatchdogAdmissionState.STOPPED
                    }
                }
            }
        }
    }

    override fun close() {
        deadlineJob?.cancel()
    }
}
