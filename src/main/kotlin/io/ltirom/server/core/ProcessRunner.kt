package io.ltirom.server.core

import io.ltirom.server.domain.ports.ProcessCancellationPort
import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.domain.ports.ProcessOutputChunk
import io.ltirom.server.domain.ports.ToolResolverPort
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlin.system.measureTimeMillis

public class LinuxToolResolver(
    private val toolsBinDir: File = File(System.getProperty("user.home"), ".ltirom/bin"),
    private val fallbackDirs: List<File> = listOf(
        File(System.getProperty("user.home"), ".local/bin"),
        File(System.getProperty("user.home"), "Android/Sdk/platform-tools"),
        File("/opt/ltirom/bin"),
        File("/usr/local/bin"),
        File("/usr/bin"),
        File("/usr/sbin"),
        File("/sbin"),
        File("/home/linuxbrew/.linuxbrew/bin")
    ),
    searchPath: String = System.getenv("PATH").orEmpty(),
) : ToolResolverPort {
    public val knownTools: Set<String> = setOf(
        "aapt2", "adb", "apktool", "append2simg", "avbtool", "dump.erofs", "e2fsdroid",
        "erofsfuse", "ext2simg", "fastboot", "fec", "fsck.erofs", "gh", "img2sdat",
        "img2simg", "lpadd", "lpdump", "lpflash", "lpmake", "lpunpack", "make_f2fs",
        "mkbootfs", "mkbootimg", "mkdtboimg", "mke2fs", "mkf2fsuserimg", "mkfs.erofs",
        "mkuserimg_mke2fs", "payload-dumper-go", "repack_bootimg", "signapk", "simg2img",
        "sload_f2fs", "unpack_bootimg", "zipalign"
    )

    override fun resolve(toolName: String): File? {
        // A full path names the file itself. File(dir, "/abs") appends instead of replacing, so without this
        // every probe of a binary by its path came back "not found" while the binary ran fine.
        File(toolName).takeIf { it.isAbsolute }?.let { return it.takeIf { f -> f.isFile && f.canExecute() } }
        val direct = File(toolsBinDir, toolName)
        if (direct.isFile && direct.canExecute()) return direct

        for (dir in fallbackDirs) {
            val f = File(dir, toolName)
            if (f.isFile && f.canExecute()) return f
        }

        for (dir in pathDirs) {
            val f = File(dir, toolName)
            if (f.isFile && f.canExecute()) return f
        }

        return null
    }

    /**
     * PATH without WSL's Windows interop entries (`/mnt/c/...`, appended from the Windows PATH): every
     * lookup there crosses the 9P bridge (dozens of folders, ~1 s per health check for 35 tools), and no
     * Linux tool lives there.
     */
    private val pathDirs: List<File> = searchPath.split(':')
        .filter { it.isNotBlank() && !isWindowsInteropDir(it) }
        .map(::File)

    public companion object {
        private val WINDOWS_DRIVE_MOUNT = Regex("^/mnt/[A-Za-z](/.*)?$")

        public fun isWindowsInteropDir(dir: String): Boolean = WINDOWS_DRIVE_MOUNT.matches(dir)
    }

    override fun listAvailableTools(): Map<String, Boolean> {
        return knownTools.associateWith { resolve(it) != null }
    }

    override fun resolveSource(toolName: String): String? {
        val direct = File(toolsBinDir, toolName)
        if (direct.isFile && direct.canExecute()) return "PINNED"
        if (resolve(toolName) != null) return "SYSTEM_PATH"
        return null
    }
}

public class LinuxProcessRunner(
    private val resolver: ToolResolverPort,
    private val limits: io.ltirom.server.domain.model.ProcessResourceLimits = io.ltirom.server.domain.model.ProcessResourceLimits()
) : ProcessExecutionPort, ProcessCancellationPort {

    private val activeProcesses = ConcurrentHashMap<String, Process>()
    override suspend fun runProcess(
        toolName: String,
        arguments: List<String>,
        workingDir: File,
        environment: Map<String, String>,
        stdinText: String?,
        timeoutMs: Long,
        onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?
    ): ProcessExecutionResult {
        val resolved = resolver.resolve(toolName)
            ?: throw IllegalArgumentException("Tool '$toolName' not found or not executable in tools directory or PATH")

        val baseCmd = when {
            resolved.name.endsWith(".jar") -> listOf("java", "-jar", resolved.absolutePath) + arguments
            resolved.name.endsWith(".py") -> listOf("python3", resolved.absolutePath) + arguments
            else -> listOf(resolved.absolutePath) + arguments
        }
        val cmd = limits.wrapCommand(baseCmd)

        require(workingDir.isAbsolute && workingDir.isDirectory) { "working directory missing" }

        val pb = ProcessBuilder(cmd)
        pb.directory(workingDir)
        pb.environment().putAll(environment)

        var process: Process? = null
        var stdout = ""
        var stderr = ""
        var exitCode = -1
        val knownDescendants = ConcurrentHashMap.newKeySet<ProcessHandle>()

        val durationMs = measureTimeMillis {
            coroutineScope {
                val p = withContext(Dispatchers.IO) { pb.start() }
                process = p

                val watcher = launch(Dispatchers.IO) {
                    while (p.isAlive) {
                        p.descendants().forEach(knownDescendants::add)
                        delay(50)
                    }
                }

                try {
                    runWithOptionalTimeout(timeoutMs) {
                        if (!stdinText.isNullOrEmpty()) {
                            launch(Dispatchers.IO) {
                                runCatching {
                                    p.outputStream.bufferedWriter().use { it.write(stdinText) }
                                }
                            }
                        }

                        val maxCaptureLength = 10 * 1024 * 1024 // 10MB memory safety cap
                        val stdoutDeferred = async(Dispatchers.IO) {
                            val sb = StringBuilder()
                            p.inputStream.bufferedReader().useLines { lines ->
                                for (line in lines) {
                                    if (sb.length < maxCaptureLength) {
                                        sb.appendLine(line)
                                    }
                                    onOutput?.invoke(line, "")
                                }
                            }
                            sb.toString().trimEnd()
                        }

                        val stderrDeferred = async(Dispatchers.IO) {
                            val sb = StringBuilder()
                            p.errorStream.bufferedReader().useLines { lines ->
                                for (line in lines) {
                                    if (sb.length < maxCaptureLength) {
                                        sb.appendLine(line)
                                    }
                                    onOutput?.invoke("", line)
                                }
                            }
                            sb.toString().trimEnd()
                        }

                        exitCode = withContext(Dispatchers.IO) { p.waitFor() }
                        stdout = stdoutDeferred.await()
                        stderr = stderrDeferred.await()
                    }
                } catch (e: TimeoutCancellationException) {
                    cleanupProcess(p, knownDescendants)
                    throw IllegalStateException("Tool '$toolName' timed out after $timeoutMs ms", e)
                } finally {
                    watcher.cancel()
                    cleanupProcess(p, knownDescendants)
                }
            }
        }

        return ProcessExecutionResult(
            exitCode = exitCode,
            stdout = stdout,
            stderr = stderr,
            durationMs = durationMs
        )
    }

    override fun runProcessFlow(
        toolName: String,
        arguments: List<String>,
        workingDir: File,
        environment: Map<String, String>,
        stdinText: String?,
        timeoutMs: Long,
        executionId: String?
    ): Flow<ProcessOutputChunk> = channelFlow {
        val resolved = resolver.resolve(toolName)
            ?: throw IllegalArgumentException("Tool '$toolName' not found or not executable in tools directory or PATH")

        val baseCmd = when {
            resolved.name.endsWith(".jar") -> listOf("java", "-jar", resolved.absolutePath) + arguments
            resolved.name.endsWith(".py") -> listOf("python3", resolved.absolutePath) + arguments
            else -> listOf(resolved.absolutePath) + arguments
        }
        val cmd = limits.wrapCommand(baseCmd)

        require(workingDir.isAbsolute && workingDir.isDirectory) { "working directory missing" }

        val pb = ProcessBuilder(cmd)
        pb.directory(workingDir)
        pb.environment().putAll(environment)

        val p = withContext(Dispatchers.IO) { pb.start() }
        val regKey = executionId ?: "exec-${p.pid()}"
        activeProcesses[regKey] = p
        val knownDescendants = ConcurrentHashMap.newKeySet<ProcessHandle>()
        val startTime = System.currentTimeMillis()

        val watcher = launch(Dispatchers.IO) {
            while (p.isAlive) {
                p.descendants().forEach(knownDescendants::add)
                delay(50)
            }
        }

        try {
            runWithOptionalTimeout(timeoutMs) {
                if (!stdinText.isNullOrEmpty()) {
                    launch(Dispatchers.IO) {
                        runCatching {
                            p.outputStream.bufferedWriter().use { it.write(stdinText) }
                        }
                    }
                }

                val stdoutJob = launch(Dispatchers.IO) {
                    p.inputStream.bufferedReader().useLines { lines ->
                        for (line in lines) {
                            send(ProcessOutputChunk.Stdout(line))
                        }
                    }
                }

                val stderrJob = launch(Dispatchers.IO) {
                    p.errorStream.bufferedReader().useLines { lines ->
                        for (line in lines) {
                            send(ProcessOutputChunk.Stderr(line))
                        }
                    }
                }

                val exitCode = withContext(Dispatchers.IO) { p.waitFor() }
                stdoutJob.join()
                stderrJob.join()

                val durationMs = System.currentTimeMillis() - startTime
                send(ProcessOutputChunk.Exit(exitCode, durationMs))
            }
        } catch (e: TimeoutCancellationException) {
            cleanupProcess(p, knownDescendants)
            throw IllegalStateException("Tool '$toolName' timed out after $timeoutMs ms", e)
        } finally {
            watcher.cancel()
            activeProcesses.remove(regKey)
            cleanupProcess(p, knownDescendants)
        }
    }

    override fun cancelProcess(executionId: String, signal: String): Boolean {
        val p = activeProcesses[executionId] ?: return false
        val pid = runCatching { p.pid() }.getOrNull() ?: return false
        val sig = if (signal.startsWith("SIG")) signal.substring(3) else signal
        return runCatching {
            ProcessBuilder("kill", "-$sig", "-$pid").start().waitFor() == 0
        }.getOrDefault(false)
    }

    private fun cleanupProcess(process: Process?, knownDescendants: MutableSet<ProcessHandle>) {
        if (process == null || !process.isAlive) return
        try {
            val pid = runCatching { process.pid() }.getOrNull()
            process.descendants().forEach(knownDescendants::add)
            // On Linux, kill process group if possible to prevent orphaned subprocesses
            if (pid != null) {
                runCatching {
                    ProcessBuilder("kill", "-TERM", "-$pid").start().waitFor()
                }
            }
            knownDescendants.toList().asReversed().forEach { runCatching { it.destroy() } }
            process.destroy()
            Thread.sleep(100)
            if (pid != null && process.isAlive) {
                runCatching {
                    ProcessBuilder("kill", "-KILL", "-$pid").start().waitFor()
                }
            }
            knownDescendants.forEach { if (it.isAlive) runCatching { it.destroyForcibly() } }
            if (process.isAlive) process.destroyForcibly()
        } catch (_: Exception) {}
    }

    private suspend fun <T> runWithOptionalTimeout(
        timeoutMs: Long,
        block: suspend CoroutineScope.() -> T
    ): T {
        return if (timeoutMs > 0) {
            withTimeout(timeoutMs, block)
        } else {
            coroutineScope(block)
        }
    }
}
