package io.ltirom.server.core

import io.ltirom.server.domain.ports.ProcessCancellationPort
import io.ltirom.server.domain.ports.ProcessExecutionPort
import io.ltirom.server.domain.ports.ProcessExecutionResult
import io.ltirom.server.domain.ports.ProcessOutputChunk
import io.ltirom.server.domain.ports.ToolResolverPort
import java.io.File
import java.io.InputStreamReader
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlin.system.measureTimeMillis

public class LinuxToolResolver(
    private val toolsBinDir: File = File(System.getProperty("user.home"), "LtiRomTools/bin"),
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

    private fun isUsableToolFile(file: File): Boolean {
        if (!file.isFile) return false
        if (file.name.endsWith(".jar") || file.name.endsWith(".py")) {
            return file.canRead()
        }
        return file.canExecute()
    }

    override fun resolve(toolName: String): File? {
        // A full path names the file itself. File(dir, "/abs") appends instead of replacing, so without this
        // every probe of a binary by its path came back "not found" while the binary ran fine.
        File(toolName).takeIf { it.isAbsolute }?.let { return it.takeIf { f -> isUsableToolFile(f) } }
        val direct = File(toolsBinDir, toolName)
        if (isUsableToolFile(direct)) return direct

        for (dir in fallbackDirs) {
            val f = File(dir, toolName)
            if (isUsableToolFile(f)) return f
        }

        for (dir in pathDirs) {
            val f = File(dir, toolName)
            if (isUsableToolFile(f)) return f
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

/** Captured output of one stream: bounded in memory, readable at any moment, honest about what was cut. */
internal class BoundedCapture(private val maxChars: Int) {
    private val buffer = StringBuilder()
    private var truncated = false

    @Synchronized
    fun add(line: String) {
        if (truncated) return
        val room = maxChars - buffer.length
        if (line.length + 1 <= room) {
            buffer.append(line).append('\n')
        } else {
            if (room > 0) buffer.append(line, 0, minOf(room, line.length))
            truncated = true
        }
    }

    @Synchronized
    fun text(drainedFully: Boolean = true): String = buffer.toString().trimEnd() +
        (if (truncated) "\n[output truncated at $maxChars characters]" else "") +
        (if (!drainedFully) "\n[output still being read when the command ended; the end may be missing]" else "")
}

private const val MAX_CAPTURE_CHARS = 10 * 1024 * 1024
private const val MAX_LINE_CHARS = 1024 * 1024

/**
 * Lines as [java.io.BufferedReader.readLine] sees them (\n, \r and \r\n end one), except that a line longer
 * than [maxLine] is handed over in pieces: a process printing gigabytes without a newline can no longer
 * allocate them all before anything checks a limit. Inline, so [action] may suspend.
 */
internal inline fun java.io.Reader.forEachBoundedLine(maxLine: Int = MAX_LINE_CHARS, action: (String) -> Unit) {
    val line = StringBuilder()
    val chunk = CharArray(8192)
    var afterCarriageReturn = false
    while (true) {
        val count = read(chunk)
        if (count < 0) break
        for (i in 0 until count) {
            val c = chunk[i]
            var emit = false
            when (c) {
                '\r' -> { emit = true; afterCarriageReturn = true }
                '\n' -> { emit = !afterCarriageReturn; afterCarriageReturn = false }
                else -> { afterCarriageReturn = false; line.append(c); emit = line.length >= maxLine }
            }
            if (emit) {
                action(line.toString())
                line.setLength(0)
            }
        }
    }
    if (line.isNotEmpty()) action(line.toString())
}

/**
 * Reads a process's output until the process has exited and everything it wrote is drained - not until the pipe
 * reaches end-of-file. A child the command left running on purpose inherits the pipe and keeps it open for as long
 * as it lives; waiting for end-of-file then made a finished command (and its run) wait for that child.
 */
internal class PollingInputStream(
    private val source: java.io.InputStream,
    private val processAlive: () -> Boolean,
) : java.io.InputStream() {
    override fun read(): Int {
        val one = ByteArray(1)
        return if (read(one, 0, 1) < 0) -1 else one[0].toInt() and BYTE_MASK
    }

    override fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        if (length == 0) return 0
        while (true) {
            val available = source.available()
            if (available > 0) return source.read(buffer, offset, minOf(length, available))
            // Exited and nothing left to read (checked again after the exit, so a last write is not missed).
            if (!processAlive() && source.available() == 0) return -1
            Thread.sleep(POLL_INTERVAL_MS)
        }
    }

    override fun available(): Int = source.available()

    override fun close() = source.close()

    private companion object {
        const val POLL_INTERVAL_MS = 10L
        const val BYTE_MASK = 0xFF
    }
}

/** A leftover process, identified by pid *and* start time: a pid alone can later belong to someone else. */
internal data class Survivor(val pid: Long, val startedAtMs: Long?) {
    /** The live process this record names, or null if it is gone or the pid now belongs to another process. */
    fun handle(): ProcessHandle? = ProcessHandle.of(pid).orElse(null)?.takeIf { candidate ->
        val actual = candidate.info().startInstant().map { it.toEpochMilli() }.orElse(null)
        startedAtMs == null || actual == null || kotlin.math.abs(actual - startedAtMs) <= START_TOLERANCE_MS
    }
}

private const val START_TOLERANCE_MS = 2_000L

/** Adds the live members of the session/group led by [rootPid] (other than [rootPid] itself) to [into]. */
private fun addGroupMembers(rootPid: Long, into: MutableSet<ProcessHandle>) {
    listOf("-s", "-g").forEach { flag ->
        runCatching {
            val pgrepProc = ProcessBuilder("pgrep", flag, rootPid.toString()).start()
            val pids = pgrepProc.inputStream.bufferedReader().readLines().mapNotNull { it.trim().toLongOrNull() }
            pgrepProc.waitFor(500, TimeUnit.MILLISECONDS)
            pids.filter { it != rootPid }.forEach { ProcessHandle.of(it).ifPresent(into::add) }
        }
    }
}

internal fun survivorOf(pid: Long): Survivor =
    Survivor(pid, ProcessHandle.of(pid).orElse(null)?.info()?.startInstant()?.map { it.toEpochMilli() }?.orElse(null))

public class LinuxProcessRunner(
    private val resolver: ToolResolverPort,
    private val limits: io.ltirom.server.domain.model.ProcessResourceLimits = io.ltirom.server.domain.model.ProcessResourceLimits()
) : ProcessExecutionPort, ProcessCancellationPort {

    private val activeProcesses = ConcurrentHashMap<String, Process>()
    // Shared with cancellation so children observed before their parent exits remain accountable.
    private val trackedDescendants = ConcurrentHashMap<Process, MutableSet<ProcessHandle>>()
    private val closer = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val processSurvivors = ConcurrentHashMap<String, List<Survivor>>()
    private val cancelledExecutionIds = ConcurrentHashMap.newKeySet<String>()

    override suspend fun runProcess(
        toolName: String,
        arguments: List<String>,
        workingDir: File,
        environment: Map<String, String>,
        stdinText: String?,
        timeoutMs: Long,
        onOutput: ((stdoutLine: String, stderrLine: String) -> Unit)?,
        executionId: String?
    ): ProcessExecutionResult {
        if (executionId != null && executionId in cancelledExecutionIds) {
            cancelledExecutionIds.remove(executionId)
            throw CancellationException("Execution '$executionId' was cancelled before process creation")
        }
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

        var stdout = ""
        var stderr = ""
        var exitCode = -1
        val knownDescendants = ConcurrentHashMap.newKeySet<ProcessHandle>()
        var regKey: String? = null
        var finalSurvivors: List<Long> = emptyList()

        val durationMs = measureTimeMillis {
            coroutineScope {
                var spawnedProcess: Process? = null
                try {
                    withContext(NonCancellable + Dispatchers.IO) {
                        val p = pb.start()
                        spawnedProcess = p
                        val key = executionId ?: "exec-${p.pid()}"
                        regKey = key
                        trackedDescendants[p] = knownDescendants
                        activeProcesses[key] = p
                    }
                } catch (ce: CancellationException) {
                    spawnedProcess?.let { p ->
                        val survivors = cleanupProcessSuspend(p, knownDescendants)
                        regKey?.let { key ->
                            if (survivors.isEmpty()) {
                                activeProcesses.remove(key)
                                processSurvivors.remove(key)
                            } else {
                                processSurvivors[key] = survivors.map(::survivorOf)
                            }
                        }
                    }
                    throw ce
                }
                val p = spawnedProcess!!

                if (executionId != null && executionId in cancelledExecutionIds || !coroutineContext.isActive) {
                    val survivors = cleanupProcessSuspend(p, knownDescendants)
                    if (survivors.isEmpty()) {
                        activeProcesses.remove(regKey!!)
                        processSurvivors.remove(regKey!!)
                    } else {
                        processSurvivors[regKey!!] = survivors.map(::survivorOf)
                    }
                    if (executionId != null) cancelledExecutionIds.remove(executionId)
                    throw CancellationException("Execution '$executionId' was cancelled during process spawn")
                }

                val watcher = launch(Dispatchers.IO) {
                    while (p.isAlive) {
                        p.descendants().forEach(knownDescendants::add)
                        delay(50)
                    }
                }

                val stdoutCapture = BoundedCapture(MAX_CAPTURE_CHARS)
                val stderrCapture = BoundedCapture(MAX_CAPTURE_CHARS)
                val stdoutDeferred = launch(Dispatchers.IO) {
                    runCatching {
                        InputStreamReader(PollingInputStream(p.inputStream) { p.isAlive }, Charsets.UTF_8).forEachBoundedLine { line ->
                            stdoutCapture.add(line)
                            onOutput?.invoke(line, "")
                        }
                    }
                }

                val stderrDeferred = launch(Dispatchers.IO) {
                    runCatching {
                        InputStreamReader(PollingInputStream(p.errorStream) { p.isAlive }, Charsets.UTF_8).forEachBoundedLine { line ->
                            stderrCapture.add(line)
                            onOutput?.invoke("", line)
                        }
                    }
                }

                val stdinJob = feedStdin(p, stdinText)

                var timedOut = false
                try {
                    runWithOptionalTimeout(timeoutMs) {
                        while (p.isAlive) {
                            delay(20)
                        }
                    }
                    exitCode = p.exitValue()
                    // Whatever was read before a slow drain gave up is kept, with a note saying so.
                    val drained = withTimeoutOrNull(15_000L) {
                        stdoutDeferred.join()
                        stderrDeferred.join()
                    } != null
                    stdout = stdoutCapture.text(drained)
                    stderr = stderrCapture.text(drained)
                } catch (e: TimeoutCancellationException) {
                    timedOut = true
                } catch (ce: CancellationException) {
                    throw ce
                } finally {
                    val wasCancelled = executionId != null && cancelledExecutionIds.contains(executionId)
                    val completedNormally = !p.isAlive && !timedOut && !wasCancelled
                    finalSurvivors = cleanupProcessSuspend(
                        process = p,
                        knownDescendants = knownDescendants,
                        watcherJob = watcher,
                        stdoutJob = stdoutDeferred,
                        stderrJob = stderrDeferred,
                        stdinJob = stdinJob,
                        killDescendants = !completedNormally,
                        toolName = toolName,
                        killStrays = exitCode != 0,
                    )
                    if (finalSurvivors.isEmpty()) {
                        activeProcesses.remove(regKey!!)
                        processSurvivors.remove(regKey!!)
                    } else {
                        processSurvivors[regKey!!] = finalSurvivors.map(::survivorOf)
                    }
                    if (executionId != null) {
                        cancelledExecutionIds.remove(executionId)
                    }
                    if (timedOut) {
                        throw IllegalStateException("Tool '$toolName' timed out after $timeoutMs ms")
                    }
                }
            }
        }

        return ProcessExecutionResult(
            exitCode = exitCode,
            stdout = stdout,
            stderr = stderr,
            durationMs = durationMs,
            survivors = finalSurvivors
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
        if (executionId != null && executionId in cancelledExecutionIds) {
            cancelledExecutionIds.remove(executionId)
            throw CancellationException("Execution '$executionId' was cancelled before process creation")
        }
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

        var spawnedProcess: Process? = null
        var regKey: String? = null
        val knownDescendants = ConcurrentHashMap.newKeySet<ProcessHandle>()
        try {
            withContext(NonCancellable + Dispatchers.IO) {
                val p = pb.start()
                spawnedProcess = p
                val key = executionId ?: "exec-${p.pid()}"
                regKey = key
                trackedDescendants[p] = knownDescendants
                activeProcesses[key] = p
            }
        } catch (ce: CancellationException) {
            spawnedProcess?.let { p ->
                val survivors = cleanupProcessSuspend(p, knownDescendants)
                regKey?.let { key ->
                    if (survivors.isEmpty()) {
                        activeProcesses.remove(key)
                        processSurvivors.remove(key)
                    } else {
                        processSurvivors[key] = survivors.map(::survivorOf)
                    }
                }
            }
            throw ce
        }
        val p = spawnedProcess!!
        val key = regKey!!

        val startTime = System.currentTimeMillis()

        if (executionId != null && executionId in cancelledExecutionIds || !coroutineContext.isActive) {
            val survivors = cleanupProcessSuspend(p, knownDescendants)
            if (survivors.isEmpty()) {
                activeProcesses.remove(key)
                processSurvivors.remove(key)
            } else {
                processSurvivors[key] = survivors.map(::survivorOf)
            }
            if (executionId != null) cancelledExecutionIds.remove(executionId)
            throw CancellationException("Execution '$executionId' was cancelled during process spawn")
        }

        var watcher: Job? = null
        var stdinJob: Job? = null
        var stdoutJob: Job? = null
        var stderrJob: Job? = null
        var timedOut = false
        var survivorsOnExit: List<Long> = emptyList()
        var completedExitCode: Int? = null
        var executionDurationMs: Long? = null
        // The process is registered, so everything from here on, the Started event included, is inside the cleanup:
        // a cancellation while that event is being delivered must still stop the command.
        try {
            val startedAt = p.toHandle().info().startInstant().map { it.toEpochMilli() }.orElse(null)
            send(ProcessOutputChunk.Started(p.pid(), startedAt))

            watcher = launch(Dispatchers.IO) {
                while (p.isAlive) {
                    p.descendants().forEach(knownDescendants::add)
                    delay(50)
                }
            }

            stdinJob = feedStdin(p, stdinText)

            stdoutJob = launch(Dispatchers.IO) {
                runCatching {
                    InputStreamReader(PollingInputStream(p.inputStream) { p.isAlive }, Charsets.UTF_8).forEachBoundedLine { line ->
                        send(ProcessOutputChunk.Stdout(line))
                    }
                }
            }

            stderrJob = launch(Dispatchers.IO) {
                runCatching {
                    InputStreamReader(PollingInputStream(p.errorStream) { p.isAlive }, Charsets.UTF_8).forEachBoundedLine { line ->
                        send(ProcessOutputChunk.Stderr(line))
                    }
                }
            }

            runWithOptionalTimeout(timeoutMs) {
                while (p.isAlive) {
                    delay(20)
                }
            }
            val exitCode = p.exitValue()
            withTimeoutOrNull(15_000L) {
                stdoutJob?.join()
                stderrJob?.join()
            }
            completedExitCode = exitCode
            executionDurationMs = System.currentTimeMillis() - startTime
        } catch (e: TimeoutCancellationException) {
            timedOut = true
        } catch (ce: CancellationException) {
            throw ce
        } finally {
            val wasCancelled = executionId != null && cancelledExecutionIds.contains(executionId)
            val shouldKill = timedOut || wasCancelled || completedExitCode == null
            survivorsOnExit = cleanupProcessSuspend(
                process = p,
                knownDescendants = knownDescendants,
                watcherJob = watcher,
                stdoutJob = stdoutJob,
                stderrJob = stderrJob,
                stdinJob = stdinJob,
                killDescendants = shouldKill,
                toolName = toolName,
                killStrays = completedExitCode != 0,
            )
            if (survivorsOnExit.isEmpty()) {
                activeProcesses.remove(key)
                processSurvivors.remove(key)
            } else {
                processSurvivors[key] = survivorsOnExit.map(::survivorOf)
            }
            if (executionId != null) {
                cancelledExecutionIds.remove(executionId)
            }
            if (completedExitCode != null && executionDurationMs != null && !timedOut) {
                send(ProcessOutputChunk.Exit(completedExitCode, executionDurationMs))
            }
            if (timedOut) {
                throw IllegalStateException("Tool '$toolName' timed out after $timeoutMs ms")
            }
        }
    }

    /**
     * Blocking compatibility boundary: at most 3 seconds of terminal observation, including
     * a 500 ms graceful phase, then forcible escalation. Dispatch alone is never success.
     * Unknown IDs are not remembered: a failed cancellation must not poison a future execution.
     */
    override fun cancelProcess(executionId: String, signal: String): Boolean {
        val p = activeProcesses[executionId]
        val descendants = p?.let { trackedDescendants[it] } ?: ConcurrentHashMap.newKeySet<ProcessHandle>()
        for (survivor in processSurvivors[executionId].orEmpty()) {
            survivor.handle()?.let(descendants::add)
        }
        if (p == null && descendants.isEmpty()) {
            return false
        }

        val sig = signal.removePrefix("SIG")
        if (!sig.matches(Regex("[A-Z]+|[0-9]+"))) return false
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
        return try {
            p?.let { proc ->
                runCatching {
                    proc.descendants().use { it.forEach(descendants::add) }
                }
            }
            cancelledExecutionIds.add(executionId)
            // A command that has just exited may have left children the run's own cleanup has not recorded yet; they
            // are still in its session, so they are found here rather than reported gone.
            p?.let { proc -> runCatching { addGroupMembers(proc.pid(), descendants) } }
            val targets = descendants.filter { it.isAlive } + (if (p?.isAlive == true) listOf(p.toHandle()) else emptyList())
            if (targets.isEmpty()) {
                activeProcesses.remove(executionId)
                if (p != null) trackedDescendants.remove(p)
                processSurvivors.remove(executionId)
                return true
            }
            val rootPid = runCatching { p?.pid() }.getOrNull()
            if (rootPid != null) {
                runCatching {
                    val kp = ProcessBuilder("kill", "-$sig", "--", "-$rootPid").start()
                    kp.waitFor(200, TimeUnit.MILLISECONDS)
                }
            }
            when (sig) {
                "TERM", "15" -> targets.forEach { if (it.isAlive) it.destroy() }
                "KILL", "9" -> targets.forEach { if (it.isAlive) it.destroyForcibly() }
                else -> {
                    val dispatcher = ProcessBuilder(
                        listOf("kill", "-$sig", "--") + targets.map { it.pid().toString() }
                    ).start()
                    if (!dispatcher.waitFor(500, TimeUnit.MILLISECONDS)) {
                        dispatcher.destroyForcibly()
                        return false
                    }
                    if (dispatcher.exitValue() != 0) return false
                }
            }
            val gracefulDeadline = minOf(deadline, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500))
            while (true) {
                // Discover new children until the original exits; retain handles after reparenting.
                p?.let { proc ->
                    runCatching {
                        proc.descendants().use { it.forEach(descendants::add) }
                    }
                }
                descendants.toList().forEach { child ->
                    runCatching {
                        if (child.isAlive) {
                            child.descendants().use { it.forEach(descendants::add) }
                        }
                    }
                }
                val living = descendants.filter { it.isAlive }
                if ((p == null || !p.isAlive) && living.isEmpty()) {
                    activeProcesses.remove(executionId)
                    if (p != null) trackedDescendants.remove(p)
                    processSurvivors.remove(executionId)
                    return true
                }
                val now = System.nanoTime()
                if (now >= deadline) {
                    val remaining = living.map { it.pid() } + (if (p?.isAlive == true) listOf(p.pid()) else emptyList())
                    if (remaining.isNotEmpty()) {
                        processSurvivors[executionId] = remaining.map(::survivorOf)
                    } else {
                        processSurvivors.remove(executionId)
                    }
                    return false
                }
                if (now >= gracefulDeadline) {
                    living.forEach { it.destroyForcibly() }
                    if (p?.isAlive == true) p.destroyForcibly()
                    if (rootPid != null) {
                        runCatching {
                            val kp = ProcessBuilder("kill", "-KILL", "--", "-$rootPid").start()
                            kp.waitFor(200, TimeUnit.MILLISECONDS)
                        }
                    }
                }
                Thread.sleep(20)
            }
            @Suppress("UNREACHABLE_CODE")
            false
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            false
        } catch (e: Exception) {
            // Failed discovery, signalling or liveness observation cannot prove termination.
            false
        }
    }

    /**
     * Gives the child its input and then end-of-file. A command that reads stdin to EOF (`cat`, `sort`, a
     * script piping from it) used to wait forever when there was no input, because the pipe was never closed.
     */
    private fun CoroutineScope.feedStdin(process: Process, stdinText: String?): Job? {
        if (stdinText.isNullOrEmpty()) {
            runCatching { process.outputStream.close() }
            return null
        }
        return launch(Dispatchers.IO) {
            runCatching { process.outputStream.bufferedWriter().use { it.write(stdinText) } }
        }
    }

    override fun getSurvivors(executionId: String): List<Long> {
        val recorded = processSurvivors[executionId] ?: return emptyList()
        val living = recorded.filter { it.handle()?.isAlive == true }
        if (living.isEmpty()) {
            processSurvivors.remove(executionId)
            activeProcesses.remove(executionId)?.let { trackedDescendants.remove(it) }
        } else if (living.size != recorded.size) {
            processSurvivors[executionId] = living
        }
        return living.map { it.pid }
    }

    public fun isPersistentHelper(handle: ProcessHandle, toolName: String): Boolean {
        if (toolName != "adb") return false
        val info = runCatching { handle.info() }.getOrNull() ?: return false
        val cmd = info.command().orElse("").lowercase()
        val cmdLine = info.commandLine().orElse("").lowercase()
        val args = info.arguments().orElse(emptyArray())
        val isAdb = cmd.endsWith("/adb") || cmd.endsWith("\\adb") || cmd == "adb" || cmdLine.contains("adb")
        val isServer = cmdLine.contains("server") || args.any { it.contains("server") }
        return if (cmdLine.isNotBlank() || args.isNotEmpty()) {
            isAdb && isServer
        } else {
            isAdb
        }
    }

    /**
     * Closing a pipe that another thread is blocked reading can itself block (it does on Windows, and a child that
     * kept the pipe open keeps the reader blocked). Nothing after this depends on the close having finished, so it
     * is done on its own thread rather than waited for.
     */
    private fun closeStreamsInBackground(p: Process) {
        closer.launch {
            runCatching { p.outputStream.close() }
            runCatching { p.inputStream.close() }
            runCatching { p.errorStream.close() }
        }
    }

    private suspend fun cleanupProcessSuspend(
        process: Process?,
        knownDescendants: MutableSet<ProcessHandle>,
        watcherJob: Job? = null,
        stdoutJob: Job? = null,
        stderrJob: Job? = null,
        stdinJob: Job? = null,
        killDescendants: Boolean = true,
        toolName: String = "",
        killStrays: Boolean = false,
    ): List<Long> = withContext(NonCancellable) {
        if (!killDescendants) {
            watcherJob?.cancel()
            withTimeoutOrNull(15_000L) {
                stdoutJob?.join()
                stderrJob?.join()
                stdinJob?.join()
            }
            process?.let(::closeStreamsInBackground)
            process?.descendants()?.use { it.forEach(knownDescendants::add) }
            runCatching { process?.pid() }.getOrNull()?.let { addGroupMembers(it, knownDescendants) }
            knownDescendants.toList().forEach { handle ->
                runCatching {
                    if (handle.isAlive) {
                        handle.descendants().use { it.forEach(knownDescendants::add) }
                    }
                }
            }
            val livingDescendants = knownDescendants.filter { it.isAlive }
            val (persistentHelpers, foundOrdinary) = livingDescendants.partition { handle ->
                isPersistentHelper(handle, toolName)
            }
            // A command that failed leaves no ordinary children behind (persistent helpers excepted); one that
            // succeeded may have started something on purpose, which is reported rather than killed.
            val ordinaryDescendants =
                if (killStrays && foundOrdinary.isNotEmpty()) {
                    foundOrdinary.forEach { runCatching { it.destroy() } }
                    withTimeoutOrNull(500L) { while (foundOrdinary.any { it.isAlive }) delay(25) }
                    foundOrdinary.filter { it.isAlive }.forEach { runCatching { it.destroyForcibly() } }
                    withTimeoutOrNull(500L) { while (foundOrdinary.any { it.isAlive }) delay(25) }
                    foundOrdinary.filter { it.isAlive }
                } else {
                    foundOrdinary
                }
            if (ordinaryDescendants.isNotEmpty()) {
                if (process != null) {
                    trackedDescendants[process] = ordinaryDescendants.toMutableSet()
                }
                return@withContext ordinaryDescendants.mapNotNull {
                    runCatching { it.pid() }.getOrNull()
                }
            }
            if (process != null) trackedDescendants.remove(process)
            return@withContext emptyList()
        }

        watcherJob?.cancel()
        stdoutJob?.cancel()
        stderrJob?.cancel()
        stdinJob?.cancel()

        process?.let(::closeStreamsInBackground)

        process?.descendants()?.forEach(knownDescendants::add)
        val pid = process?.let { runCatching { it.pid() }.getOrNull() }
        // The command may already have exited with a child still running (it is then no descendant of anything);
        // its session still names it, so cancellation and timeouts reach it too.
        pid?.let { addGroupMembers(it, knownDescendants) }

        // A group leader that is gone may still have a group: members forked after the scan above are only reachable
        // by signalling the group itself.
        if (pid != null && (process.isAlive || limits.isolatedProcessGroup)) {
            runCatching {
                val kp = ProcessBuilder("kill", "-TERM", "-$pid").start()
                kp.waitFor(500, TimeUnit.MILLISECONDS)
                if (kp.isAlive) {
                    kp.destroyForcibly()
                    kp.waitFor(200, TimeUnit.MILLISECONDS)
                }
            }
        }
        knownDescendants.forEach { if (it.isAlive) runCatching { it.destroy() } }
        if (process?.isAlive == true) runCatching { process.destroy() }

        withTimeoutOrNull(500L) {
            while ((process?.isAlive == true) || knownDescendants.any { it.isAlive }) {
                delay(25)
            }
        }

        if (pid != null && (process?.isAlive == true || limits.isolatedProcessGroup)) {
            runCatching {
                val kp = ProcessBuilder("kill", "-KILL", "-$pid").start()
                kp.waitFor(500, TimeUnit.MILLISECONDS)
                if (kp.isAlive) {
                    kp.destroyForcibly()
                    kp.waitFor(200, TimeUnit.MILLISECONDS)
                }
            }
        }
        pid?.let { addGroupMembers(it, knownDescendants) }
        knownDescendants.forEach { if (it.isAlive) runCatching { it.destroyForcibly() } }
        if (process?.isAlive == true) runCatching { process.destroyForcibly() }

        withTimeoutOrNull(500L) {
            while ((process?.isAlive == true) || knownDescendants.any { it.isAlive }) {
                delay(25)
            }
        }

        withTimeoutOrNull(500L) {
            watcherJob?.join()
            stdoutJob?.join()
            stderrJob?.join()
            stdinJob?.join()
        }

        val survivors = mutableListOf<Long>()
        if (process?.isAlive == true) {
            runCatching { process.pid() }.getOrNull()?.let { survivors.add(it) }
        }
        knownDescendants.filter { it.isAlive }.forEach {
            runCatching { it.pid() }.getOrNull()?.let { pidVal -> survivors.add(pidVal) }
        }
        if (survivors.isEmpty() && process != null) trackedDescendants.remove(process)
        survivors
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
