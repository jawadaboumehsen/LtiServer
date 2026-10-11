package io.ltirom.server.engine

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull

/** Asks each item of active work (as listed by [LtiRomServerEngine.activeWork]) to stop. */
public fun LtiRomServerEngine.cancelWork(ids: List<String>, signal: String = "SIGTERM") {
    for (id in ids) {
        if (id.startsWith("execute:") || id.startsWith("stream:")) {
            cancel(id.substringAfter(':'), signal)
        } else {
            cancelRun(id, signal)
            cancel(id, signal)
        }
    }
}

/**
 * [cancelWork] with a deadline: each request is made on its own thread, so one slow cancellation neither delays
 * the others nor the caller beyond [budgetMs]. A request that is still running at the deadline is left to finish.
 */
public suspend fun LtiRomServerEngine.cancelWorkWithin(ids: List<String>, budgetMs: Long, signal: String = "SIGTERM") {
    if (ids.isEmpty()) return
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    try {
        val requests = ids.map { id -> scope.launch { runCatching { cancelWork(listOf(id), signal) } } }
        withTimeoutOrNull(budgetMs) { requests.joinAll() }
    } finally {
        scope.cancel()
    }
}

/**
 * Stops [ids] within one [waitMs] budget for everything: a polite request first, then - for what is still
 * running after half of it - a kill. Returns what is still active at the end: a forced shutdown must not report
 * success, or exit, while children are left behind.
 */
public suspend fun LtiRomServerEngine.cancelWorkAndWait(ids: List<String>, waitMs: Long): List<String> {
    val deadline = System.nanoTime() + waitMs * NANOS_PER_MS
    val politeUntil = System.nanoTime() + waitMs * NANOS_PER_MS / 2
    cancelWorkWithin(ids, waitMs / 2)
    while (activeWork().isNotEmpty() && System.nanoTime() < politeUntil) delay(POLL_MS)
    val stubborn = activeWork()
    if (stubborn.isNotEmpty()) {
        val left = ((deadline - System.nanoTime()) / NANOS_PER_MS).coerceAtLeast(0L)
        cancelWorkWithin(stubborn, left / 2, signal = "SIGKILL")
        while (activeWork().isNotEmpty() && System.nanoTime() < deadline) delay(POLL_MS)
    }
    return activeWork()
}

private const val NANOS_PER_MS = 1_000_000L
private const val POLL_MS = 50L
