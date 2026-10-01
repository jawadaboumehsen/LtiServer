package io.ltirom.server.domain.model

/**
 * Resource limits and sandboxing configuration for server-side process execution.
 * Enforces memory ceilings and process-group isolation (setsid) to prevent zombie orphans.
 */
public data class ProcessResourceLimits(
    val maxMemoryBytes: Long = 4L * 1024 * 1024 * 1024, // 4GB max
    val maxOpenFiles: Long = 4096L,
    val timeoutMs: Long = 900_000L,
    val isolatedProcessGroup: Boolean = true
) {
    public val hasDeadline: Boolean get() = timeoutMs > 0

    public fun wrapCommand(baseCmd: List<String>): List<String> {
        val cmd = mutableListOf<String>()
        if (isolatedProcessGroup) {
            cmd.add("setsid")
        }
        cmd.addAll(baseCmd)
        return cmd
    }
}
