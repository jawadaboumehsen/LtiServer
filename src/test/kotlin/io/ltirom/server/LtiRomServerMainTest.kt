package io.ltirom.server

import java.io.File
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class LtiRomServerMainTest {
    @Test
    fun `an existing legacy bin directory stops startup with explicit error`() {
        val toolsHome = File(System.getProperty("user.home"), "LtiRomTools")
        val toolsDir = File(toolsHome, "bin")
        toolsDir.mkdirs()

        // Ensure it's not a symlink
        assertTrue(toolsDir.exists())

        try {
            val exception = assertFailsWith<IllegalStateException> {
                io.ltirom.server.main(arrayOf("--toolchain-layout-v2", "true"))
            }
            assertTrue(
                exception.message!!.contains("Startup failed: ~/LtiRomTools/bin exists but is a directory, not a managed toolchain symlink")
            )
        } finally {
            toolsDir.delete()
        }
    }
}
