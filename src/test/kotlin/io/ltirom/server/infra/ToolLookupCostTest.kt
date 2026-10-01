package io.ltirom.server.infra

import io.ltirom.server.core.LinuxToolResolver
import io.ltirom.server.domain.ports.ToolResolverPort
import java.io.File
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** `/health` and `/tools` run on every setup check, so their tool lookups must stay cheap. */
class ToolLookupCostTest {

    @Test
    fun `windows drive mounts are recognised, linux folders are not`() {
        assertTrue(LinuxToolResolver.isWindowsInteropDir("/mnt/c/Windows/system32"))
        assertTrue(LinuxToolResolver.isWindowsInteropDir("/mnt/d"))
        assertFalse(LinuxToolResolver.isWindowsInteropDir("/usr/bin"))
        assertFalse(LinuxToolResolver.isWindowsInteropDir("/mnt/wsl/shared"))
        assertFalse(LinuxToolResolver.isWindowsInteropDir("/mnt/data/bin"))
    }

    @Test
    fun `a tool on a linux PATH folder is still found when windows folders come first`() {
        val dir = createTempDirectory("ltirom-path").toFile()
        try {
            val tool = File(dir, "mytool").apply { writeText("#!/bin/sh\n"); setExecutable(true) }
            val resolver = LinuxToolResolver(
                toolsBinDir = File(dir, "none"),
                fallbackDirs = emptyList(),
                searchPath = "/mnt/c/Windows:/mnt/c/Program Files/Git/cmd:${dir.absolutePath}",
            )
            assertEquals(tool.absolutePath, resolver.resolve("mytool")?.absolutePath)
        } finally {
            dir.deleteRecursively()
        }
    }

    @Test
    fun `a tool named by its full path resolves to that file`() {
        val dir = createTempDirectory("ltirom-abs").toFile()
        try {
            val tool = File(dir, "lpmake").apply { writeText("#!/bin/sh\n"); setExecutable(true) }
            val resolver = LinuxToolResolver(
                toolsBinDir = File(dir, "bin"),
                fallbackDirs = emptyList(),
                searchPath = "",
            )
            // The app probes built tools by path (bin/lpmake); this used to answer "not found".
            assertEquals(tool.absolutePath, resolver.resolve(tool.absolutePath)?.absolutePath)
            assertEquals(null, resolver.resolve(File(dir, "missing").absolutePath))
        } finally {
            dir.deleteRecursively()
        }
    }

    @Test
    fun `listing tool statuses scans the fallback tools once, not once per tool`() {
        var scans = 0
        val fallback = object : ToolResolverPort {
            val known = (1..30).map { "tool$it" }
            override fun resolve(toolName: String): File? = null
            override fun listAvailableTools(): Map<String, Boolean> {
                scans++
                return known.associateWith { false }
            }
        }
        val registry = DynamicToolRegistry(fallback, pluginDir = File("does-not-exist"))

        val statuses = registry.listToolStatuses()

        assertEquals(30, statuses.size)
        assertEquals(1, scans, "one scan per listing; per-tool scans made /tools grow with the square of the tools")
    }
}
