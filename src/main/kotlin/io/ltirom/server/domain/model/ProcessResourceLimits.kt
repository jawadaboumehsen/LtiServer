package io.ltirom.server.domain.model

/**
 * Limits and sandboxing for server-side process execution.
 *
 * Enforced, in the production (isolated) mode: a new session via `setsid` so a run's processes form one group, and
 * a ceiling on open files. A memory ceiling is deliberately not offered: `ulimit -v` limits address space, which
 * breaks JVMs and compilers that reserve far more than they use, and a real limit needs cgroups the service does
 * not have the privileges for.
 */
public data class ProcessResourceLimits(
    val maxOpenFiles: Long = 4096L,
    val timeoutMs: Long = 900_000L,
    val isolatedProcessGroup: Boolean = true
) {
    public val hasDeadline: Boolean get() = timeoutMs > 0

    public fun wrapCommand(baseCmd: List<String>): List<String> {
        if (!isolatedProcessGroup) return baseCmd
        // `ulimit -n` can only lower what the process may open from here on; the command replaces the shell.
        val limitThenExec = "ulimit -n $maxOpenFiles 2>/dev/null; exec \"\$@\""
        return listOf("setsid", "sh", "-c", limitThenExec, "sh") + baseCmd
    }
}
