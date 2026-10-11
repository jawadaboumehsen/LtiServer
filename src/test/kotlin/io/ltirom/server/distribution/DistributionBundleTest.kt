package io.ltirom.server.distribution

import java.io.File
import kotlin.test.Test
import kotlin.test.assertFalse

class DistributionBundleTest {

    @Test
    fun `helper directory is not bundled into server distribution`() {
        val projectDir = File(System.getProperty("user.dir"))
        val baseBuild = if (projectDir.name == "LtiRomServer") {
            File(projectDir, "build/install")
        } else {
            File(projectDir, "LtiRomServer/build/install")
        }
        val possibleHelperDirs = listOf(
            File(baseBuild, "ltirom-server/helpers"),
            File(baseBuild, "LtiRomServer/helpers"),
        )
        for (dir in possibleHelperDirs) {
            assertFalse(dir.exists(), "helpers directory at ${dir.path} must not be present in distribution bundle")
        }
    }
}
