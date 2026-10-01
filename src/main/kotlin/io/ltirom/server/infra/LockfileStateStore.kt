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
            stateFile.writeText(jsonText)
        }
    }

    public fun deleteState() {
        runCatching { stateFile.delete() }
    }
}
