package io.ltirom.server.infra

import java.io.File
import java.io.OutputStream
import java.nio.channels.Channels
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.LinkOption.NOFOLLOW_LINKS
import java.nio.file.Path
import java.nio.file.SecureDirectoryStream
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption.CREATE_NEW
import java.nio.file.StandardOpenOption.WRITE
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.util.UUID

/**
 * Where an upload may be written, decided without touching the filesystem.
 *
 * The nearest existing ancestor is resolved to its real path (so a symlinked directory cannot lead out of the
 * allowed roots) and the not-yet-existing remainder is appended. Nothing is created here: a request that ends in
 * "forbidden" must not have changed anything. The final name itself may be a symlink - the write replaces that
 * entry (see [writeReplacing]) and never follows it.
 */
public fun planUpload(destination: File, allowedRoots: List<Path>): Path? {
    val target = destination.toPath().toAbsolutePath().normalize()
    val fileName = target.fileName ?: return null
    val parent = target.parent ?: return null
    var existing: Path = parent
    while (!Files.exists(existing)) existing = existing.parent ?: return null
    val realExisting = runCatching { existing.toRealPath() }.getOrNull() ?: return null
    // The part of the parent that does not exist yet cannot contain a symlink, so it is appended as it is.
    val realTarget = realExisting.resolve(existing.relativize(parent)).resolve(fileName.toString()).normalize()
    val inside = allowedRoots.any { realTarget.startsWith(it) }
    val isDirectory = Files.isDirectory(realTarget, java.nio.file.LinkOption.NOFOLLOW_LINKS)
    return realTarget.takeIf { inside && !isDirectory }
}

/**
 * Writes through a temporary file in the same directory and moves it over [target]. A write that fails half way
 * (disconnect, full disk) leaves the previous file untouched, and a symlink at [target] is replaced rather than
 * followed, so its destination is never truncated.
 *
 * [target] is a path from [planUpload] (no symlinks in it). The parent directory is opened once and checked to be
 * that very directory; the temporary file, the write and the move then all go through that handle, so replacing an
 * ancestor with a symlink after the plan cannot redirect any of them. (Directories that do not exist yet are created
 * by path - the JDK has no handle-relative mkdir - so a lost race can leave an empty directory behind, never a file.)
 */
public suspend fun writeReplacing(target: Path, write: suspend (OutputStream) -> Long): Long {
    val parent = target.parent
    Files.createDirectories(parent)
    Files.newDirectoryStream(parent).use { stream ->
        // ponytail: without handle-relative operations (non-POSIX hosts) this falls back to the path-based write.
        val dir = stream as? SecureDirectoryStream<Path> ?: return writeByPath(target, write)
        val opened = dir.getFileAttributeView(BasicFileAttributeView::class.java).readAttributes().fileKey()
        val planned = Files.readAttributes(parent, BasicFileAttributes::class.java, LinkOption.NOFOLLOW_LINKS).fileKey()
        if (parent.toRealPath() != parent || opened != planned) {
            throw java.io.IOException("Upload directory changed since it was checked: $parent")
        }
        val name = target.fileName
        val temporary = Path.of(".upload-${UUID.randomUUID()}.part")
        try {
            val written = dir.newByteChannel(temporary, setOf(CREATE_NEW, WRITE, NOFOLLOW_LINKS)).use { channel ->
                write(Channels.newOutputStream(channel))
            }
            dir.move(temporary, dir, name)
            return written
        } catch (e: Throwable) {
            runCatching { dir.deleteFile(temporary) }
            throw e
        }
    }
}

private suspend fun writeByPath(target: Path, write: suspend (OutputStream) -> Long): Long {
    val temporary = Files.createTempFile(target.parent, ".upload-", ".part")
    try {
        val written = Files.newOutputStream(temporary).use { write(it) }
        Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        return written
    } catch (e: Throwable) {
        runCatching { Files.deleteIfExists(temporary) }
        throw e
    }
}
