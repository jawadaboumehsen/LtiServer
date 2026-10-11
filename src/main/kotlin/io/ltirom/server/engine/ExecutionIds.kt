package io.ltirom.server.engine

/** An id a client chose (trace id, session id) that cannot name a file or collide safely. */
public class InvalidExecutionIdException(id: String) :
    IllegalArgumentException("Invalid execution id '${id.take(40)}'")

/** Another command or run is already using this id; each process is tracked and cancelled by it. */
public class DuplicateExecutionException(id: String) : IllegalStateException("Execution '$id' is already active")

private val SAFE_EXECUTION_ID = Regex("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")

/**
 * Ids end up as process keys and as folder names of the journal, so only plain names pass: no separators, no
 * leading dot, bounded length. Clients send UUIDs.
 */
public fun requireSafeExecutionId(id: String): String {
    if (!SAFE_EXECUTION_ID.matches(id)) throw InvalidExecutionIdException(id)
    return id
}
