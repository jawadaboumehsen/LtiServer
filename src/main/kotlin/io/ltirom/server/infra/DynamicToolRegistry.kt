package io.ltirom.server.infra

import io.ltirom.server.domain.ports.ToolResolverPort
import io.ltirom.server.model.ToolStatusInfo
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.File

@Serializable
public data class ToolManifest(
    val name: String,
    val executable: String,
    val description: String? = null,
    val owner: String? = null
)

/**
 * Service Provider Interface (SPI) for dynamic tool registration.
 * Scans ~/.ltirom/tools.d/ for tool manifests (.json) and merges them with
 * the base [fallbackResolver].
 */
public class DynamicToolRegistry(
    private val fallbackResolver: ToolResolverPort,
    private val pluginDir: File = File(System.getProperty("user.home"), ".ltirom/tools.d"),
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val isExecutableCheck: (File) -> Boolean = { it.isFile && it.canExecute() },
    private val isReadableCheck: (File) -> Boolean = { it.isFile && it.canRead() }
) : ToolResolverPort {

    // Replaced whole on every refresh, never mutated in place: a refresh (every client check triggers one)
    // must not let a concurrent resolve() for a running job see a half-cleared registry.
    @Volatile
    private var dynamicTools: Map<String, File> = emptyMap()

    @Volatile
    private var dynamicReasons: Map<String, String> = emptyMap()

    init {
        refresh()
    }

    @Synchronized
    public fun refresh() {
        val tools = mutableMapOf<String, File>()
        val reasons = mutableMapOf<String, String>()
        scanInto(tools, reasons)
        dynamicTools = tools
        dynamicReasons = reasons
    }

    private fun scanInto(dynamicTools: MutableMap<String, File>, dynamicReasons: MutableMap<String, String>) {
        if (!pluginDir.isDirectory) return

        val files = pluginDir.listFiles { _, name -> name.endsWith(".json") }.orEmpty()
        for (f in files) {
            val manifest = try {
                json.decodeFromString<ToolManifest>(f.readText())
            } catch (e: Exception) {
                dynamicReasons[f.nameWithoutExtension] = "manifest invalid"
                continue
            }

            if (manifest.name.isBlank() || manifest.executable.isBlank()) {
                val toolName = manifest.name.ifBlank { f.nameWithoutExtension }
                dynamicReasons[toolName] = "manifest invalid"
                continue
            }

            val exec = File(manifest.executable)
            if (!exec.exists()) {
                dynamicReasons[manifest.name] = "executable not found"
                continue
            }

            val isJar = manifest.executable.endsWith(".jar")
            if (isJar) {
                if (isReadableCheck(exec)) {
                    dynamicTools[manifest.name] = exec
                } else {
                    dynamicReasons[manifest.name] = "not readable"
                }
            } else {
                if (isExecutableCheck(exec)) {
                    dynamicTools[manifest.name] = exec
                } else {
                    dynamicReasons[manifest.name] = "not executable"
                }
            }
        }
    }

    /**
     * [fallbackAvailability] is the fallback's [ToolResolverPort.listAvailableTools] when the caller already
     * has it; listing every tool passes it once instead of re-scanning all tools per tool.
     */
    public fun getToolStatus(
        toolName: String,
        fallbackAvailability: Map<String, Boolean>? = null,
    ): ToolStatusInfo? {
        val dynamicFile = dynamicTools[toolName]
        if (dynamicFile != null) {
            val isJar = dynamicFile.name.endsWith(".jar")
            val installed = if (isJar) isReadableCheck(dynamicFile) else isExecutableCheck(dynamicFile)
            return ToolStatusInfo(
                tool = toolName,
                installed = installed,
                path = dynamicFile.absolutePath,
                source = "DYNAMIC",
                reason = null
            )
        }

        val dynamicReason = dynamicReasons[toolName]
        val fallbackFile = fallbackResolver.resolve(toolName)
        if (fallbackFile != null) {
            val fallbackSource = fallbackResolver.resolveSource(toolName)
                ?: if (fallbackFile.path.replace('\\', '/').contains(".ltirom/bin")) "PINNED" else "SYSTEM_PATH"
            return ToolStatusInfo(
                tool = toolName,
                installed = true,
                path = fallbackFile.absolutePath,
                source = fallbackSource,
                reason = dynamicReason
            )
        }

        if (dynamicReason != null) {
            return ToolStatusInfo(
                tool = toolName,
                installed = false,
                path = "",
                source = null,
                reason = dynamicReason
            )
        }

        val fallbackInstalled = (fallbackAvailability ?: fallbackResolver.listAvailableTools())[toolName]
        if (fallbackInstalled != null) {
            return ToolStatusInfo(
                tool = toolName,
                installed = fallbackInstalled,
                path = if (fallbackInstalled) fallbackResolver.resolve(toolName)?.absolutePath.orEmpty() else "",
                source = if (fallbackInstalled) fallbackResolver.resolveSource(toolName) else null,
                reason = null
            )
        }

        return null
    }

    override fun resolve(toolName: String): File? {
        // 1. Check dynamic plugins first
        dynamicTools[toolName]?.let { return it }

        // 2. Delegate to fallback resolver (pinned tools + standard PATH)
        return fallbackResolver.resolve(toolName)
    }

    override fun listAvailableTools(): Map<String, Boolean> {
        val base = fallbackResolver.listAvailableTools().toMutableMap()
        for ((name, file) in dynamicTools) {
            base[name] = if (file.name.endsWith(".jar")) isReadableCheck(file) else isExecutableCheck(file)
        }
        return base
    }

    override fun resolveSource(toolName: String): String? {
        if (dynamicTools.containsKey(toolName)) return "DYNAMIC"
        return fallbackResolver.resolveSource(toolName)
    }

    override fun listToolStatuses(): List<ToolStatusInfo> {
        val names = linkedSetOf<String>()
        names.addAll(dynamicTools.keys)
        names.addAll(dynamicReasons.keys)
        val fallbackAvailability = fallbackResolver.listAvailableTools()
        names.addAll(fallbackAvailability.keys)
        return names.mapNotNull { getToolStatus(it, fallbackAvailability) }
    }
}
