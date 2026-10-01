package io.ltirom.server.infra

import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.util.*
import java.util.UUID

public val TraceIdKey: AttributeKey<String> = AttributeKey("LtiRomTraceId")

public object TraceHeaders {
    public const val TRACE_ID: String = "X-Trace-Id"
    public const val SPAN_ID: String = "X-Span-Id"
}

/**
 * Extracts or generates distributed trace identifiers for incoming requests.
 */
public fun ApplicationCall.resolveTraceId(): String {
    val fromAttr = attributes.getOrNull(TraceIdKey)
    if (fromAttr != null) return fromAttr

    val fromHeader = request.header(TraceHeaders.TRACE_ID)
    val fromQuery = request.queryParameters["traceId"]
    val traceId = fromHeader ?: fromQuery ?: UUID.randomUUID().toString().replace("-", "")

    attributes.put(TraceIdKey, traceId)
    response.headers.append(TraceHeaders.TRACE_ID, traceId)
    return traceId
}
