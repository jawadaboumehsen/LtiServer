package io.ltirom.server.infra

import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class LockfileStateStoreTest {
    @Test
    fun `a retired service leaves the newer service's state file alone`() {
        val file = Files.createTempDirectory("state").resolve("server.state").toFile()
        val store = LockfileStateStore(file)
        store.saveState(port = 1, token = "new", pid = 120063, distro = "Ubuntu")

        store.deleteState(89086) // the old service shutting down
        assertTrue(file.exists(), "the file belongs to the running service")

        store.deleteState(120063)
        assertFalse(file.exists())
    }
}
