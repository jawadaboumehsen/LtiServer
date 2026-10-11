package io.ltirom.server.engine

import kotlinx.serialization.Serializable

@Serializable
public sealed interface Hold {
    public val label: String

    @Serializable
    public data object Shutdown : Hold {
        override val label: String get() = "SHUTDOWN"
        override fun toString(): String = label
    }

    @Serializable
    public data object ClientLifecycle : Hold {
        override val label: String get() = "CLIENT_LIFECYCLE"
        override fun toString(): String = label
    }

}
