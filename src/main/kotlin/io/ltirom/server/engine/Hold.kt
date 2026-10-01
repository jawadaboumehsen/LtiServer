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
    public data class Activation(val requestId: String) : Hold {
        override val label: String get() = "ACTIVATION($requestId)"
        override fun toString(): String = label
    }
}
