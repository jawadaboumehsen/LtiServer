package io.ltirom.server.infra

import java.io.File

/**
 * Infrastructure service responsible for persisting and cleaning up the
 * ~/.ltirom/server.state lockfile for client re-attachment.
 */
public class LockfileStateStore(
    private val stateFile: File = File(System.getProperty("user.home"), ".ltirom/server.state")
) {
    public fun saveState(port: Int, token: String, pid: Long, distro: String) {
        runCatching {
            stateFile.parentFile?.mkdirs()
            val jsonText = """{"port":$port,"token":"$token","pid":$pid,"distro":"$distro","startTimeEpochMs":${System.currentTimeMillis()}}"""
            // Atomic: the app reads this file at any moment, and a half-written one reads as "no service".
            val tmp = File(stateFile.path + ".tmp")
            tmp.writeText(jsonText)
            java.nio.file.Files.move(
                tmp.toPath(),
                stateFile.toPath(),
                java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE,
            )
        }
    }

    /**
     * Deletes the file only while it still names [pid]. A retired service used to delete the file a newer
     * service had already written, so the app found no service and launched yet another one.
     */
    public fun deleteState(pid: Long) {
        runCatching {
            if (stateFile.readText().contains("\"pid\":$pid,")) stateFile.delete()
        }
    }
}
