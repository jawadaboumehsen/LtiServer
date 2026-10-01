package io.ltirom.server.infra

import io.ltirom.server.domain.ports.ToolResolverPort
import java.io.File
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class DynamicToolRegistryTest {

    private class BaseResolver(
        private val tools: Map<String, File> = mapOf("mke2fs" to File("/bin/mke2fs")),
        private val sources: Map<String, String> = mapOf("mke2fs" to "SYSTEM_PATH")
    ) : ToolResolverPort {
        override fun resolve(toolName: String): File? = tools[toolName]

        override fun listAvailableTools(): Map<String, Boolean> =
            tools.mapValues { true }

        override fun resolveSource(toolName: String): String? = sources[toolName]
    }

    @Test
    fun `resolves fallback tools when no plugin directory exists`() {
        val base = BaseResolver()
        val registry = DynamicToolRegistry(
            fallbackResolver = base,
            pluginDir = File("nonexistent_tools_dir")
        )

        assertNotNull(registry.resolve("mke2fs"))
        assertNull(registry.resolve("custom_tool"))
        assertEquals(mapOf("mke2fs" to true), registry.listAvailableTools())
    }

    @Test
    fun `a dynamic manifest resolves a symlink to an executable file`() {
        val tempDir = Files.createTempDirectory("dyn_symlink_test").toFile()
        try {
            val toolsDir = File(tempDir, "tools.d").apply { mkdirs() }
            val binDir = File(tempDir, "bin").apply { mkdirs() }

            val targetFile = File(binDir, "actual_tool.sh").apply {
                writeText("#!/bin/sh\necho hi\n")
                setExecutable(true)
            }

            val symlinkFile = File(binDir, "symlink_tool")
            val symlinkCreated = try {
                Files.createSymbolicLink(symlinkFile.toPath(), targetFile.toPath())
                true
            } catch (_: Exception) {
                false
            }

            val executableFile = if (symlinkCreated) symlinkFile else targetFile
            val manifestFile = File(toolsDir, "custom_symlink.json")
            manifestFile.writeText(
                """
                {
                    "name": "custom_symlink",
                    "executable": "${executableFile.absolutePath.replace("\\", "\\\\")}",
                    "description": "Published binary for custom_symlink"
                }
                """.trimIndent()
            )

            val registry = DynamicToolRegistry(
                fallbackResolver = BaseResolver(),
                pluginDir = toolsDir,
                isExecutableCheck = { true }
            )

            val status = registry.getToolStatus("custom_symlink")
            assertNotNull(status)
            assertEquals(true, status.installed)
            assertEquals("DYNAMIC", status.source)
            assertNull(status.reason)
            assertEquals(executableFile.absolutePath, status.path)
        } finally {
            tempDir.deleteRecursively()
        }
    }

    @Test
    fun `a native manifest to a non-executable file gives reason not executable`() {
        val tempDir = Files.createTempDirectory("dyn_nonexec_test").toFile()
        try {
            val toolsDir = File(tempDir, "tools.d").apply { mkdirs() }
            val fakeExec = File(tempDir, "not_exec.bin").apply {
                writeText("data")
            }

            File(toolsDir, "native_bad.json").writeText(
                """
                {
                    "name": "native_bad",
                    "executable": "${fakeExec.absolutePath.replace("\\", "\\\\")}"
                }
                """.trimIndent()
            )

            val registry = DynamicToolRegistry(
                fallbackResolver = BaseResolver(tools = emptyMap()),
                pluginDir = toolsDir,
                isExecutableCheck = { false }
            )

            val status = registry.getToolStatus("native_bad")
            assertNotNull(status)
            assertEquals(false, status.installed)
            assertEquals("not executable", status.reason)
        } finally {
            tempDir.deleteRecursively()
        }
    }

    @Test
    fun `a jar manifest to a readable non-executable jar gives installed true`() {
        val tempDir = Files.createTempDirectory("dyn_jar_test").toFile()
        try {
            val toolsDir = File(tempDir, "tools.d").apply { mkdirs() }
            val jarFile = File(tempDir, "signapk.jar").apply {
                writeText("fake jar content")
            }

            File(toolsDir, "signapk.json").writeText(
                """
                {
                    "name": "signapk",
                    "executable": "${jarFile.absolutePath.replace("\\", "\\\\")}",
                    "description": "Published binary for signapk"
                }
                """.trimIndent()
            )

            val registry = DynamicToolRegistry(
                fallbackResolver = BaseResolver(tools = emptyMap()),
                pluginDir = toolsDir,
                isExecutableCheck = { false }, // Not executable
                isReadableCheck = { true }      // But readable
            )

            val status = registry.getToolStatus("signapk")
            assertNotNull(status)
            assertEquals(true, status.installed)
            assertEquals("DYNAMIC", status.source)
            assertNull(status.reason)
            assertEquals(jarFile.absolutePath, status.path)
        } finally {
            tempDir.deleteRecursively()
        }
    }

    @Test
    fun `when the dynamic manifest fails but the fallback finds a same-named tool, result keeps dynamic reason and reports fallback source and path`() {
        val tempDir = Files.createTempDirectory("dyn_fallback_test").toFile()
        try {
            val toolsDir = File(tempDir, "tools.d").apply { mkdirs() }
            val nonExec = File(tempDir, "mke2fs_broken").apply {
                writeText("broken")
            }

            File(toolsDir, "mke2fs.json").writeText(
                """
                {
                    "name": "mke2fs",
                    "executable": "${nonExec.absolutePath.replace("\\", "\\\\")}"
                }
                """.trimIndent()
            )

            val fallbackFile = File("/usr/bin/mke2fs")
            val base = BaseResolver(
                tools = mapOf("mke2fs" to fallbackFile),
                sources = mapOf("mke2fs" to "SYSTEM_PATH")
            )

            val registry = DynamicToolRegistry(
                fallbackResolver = base,
                pluginDir = toolsDir,
                isExecutableCheck = { false } // Dynamic file not executable
            )

            val status = registry.getToolStatus("mke2fs")
            assertNotNull(status)
            assertEquals(true, status.installed)
            assertEquals(fallbackFile.absolutePath, status.path)
            assertEquals("SYSTEM_PATH", status.source)
            assertEquals("not executable", status.reason)
        } finally {
            tempDir.deleteRecursively()
        }
    }
}
