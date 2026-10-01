package io.ltirom.server.domain.model

import kotlinx.serialization.Serializable

@Serializable
public enum class ToolCapability {
    EXTRACT_SPARSE,
    FIRMWARE_UNPACK,
    FLASH_PARTITION,
    FILESYSTEM_FSCK,
    GENERAL_EXECUTION
}

/**
 * Metadata descriptor for a tool registered in the server SPI.
 */
@Serializable
public data class ToolDescriptor(
    val id: String,
    val minVersion: String? = null,
    val capabilities: Set<ToolCapability> = setOf(ToolCapability.GENERAL_EXECUTION),
    val description: String? = null
)
