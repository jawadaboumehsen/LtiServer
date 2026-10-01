import java.security.MessageDigest

plugins {
    kotlin("jvm") version "2.1.0"
    kotlin("plugin.serialization") version "2.1.0"
    application
}

repositories {
    mavenCentral()
}

application {
    mainClass.set("io.ltirom.server.LtiRomServerMainKt")
}

dependencies {
    val ktorVersion = "3.0.3"
    val coroutinesVersion = "1.10.1"
    val serializationVersion = "1.8.0"

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:$serializationVersion")

    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-cio:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-server-websockets:$ktorVersion")
    implementation("io.ktor:ktor-server-cors:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json:$ktorVersion")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:$coroutinesVersion")
    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")
    testImplementation("io.ktor:ktor-client-content-negotiation:$ktorVersion")
}

kotlin {
    jvmToolchain(21)
}

version = "1.0.0"

tasks.withType<Jar> {
    manifest {
        attributes["Main-Class"] = "io.ltirom.server.LtiRomServerMainKt"
    }
}

val generateServerVersion by tasks.registering {
    val outputDir = layout.buildDirectory.dir("generated/resources/version")
    // Captured at configuration time: `project` is not available to doLast under the configuration cache.
    val serverVersion = project.version.toString()
    val repoDir = project.rootDir
    inputs.property("serverVersion", serverVersion)
    outputs.dir(outputDir)
    outputs.upToDateWhen { false } // the git commit is part of the version
    doLast {
        val dir = outputDir.get().asFile
        dir.mkdirs()
        fun git(vararg args: String): String = runCatching {
            val proc = ProcessBuilder("git", *args).directory(repoDir).start()
            val text = proc.inputStream.bufferedReader().readText().trim()
            proc.waitFor()
            text
        }.getOrDefault("")

        val gitCommit = git("rev-parse", "--short", "HEAD").ifEmpty { "dev" }
        // Uncommitted server changes get a content hash: the app installs a bundle only when its version
        // differs from the installed one, so the same commit with different code must not share a version.
        val dirty = git("status", "--porcelain", "--", ".").isNotEmpty()
        val sourceHash = if (dirty) {
            val digest = MessageDigest.getInstance("SHA-256")
            repoDir.resolve("src").walkTopDown().filter { it.isFile }
                .sortedBy { it.relativeTo(repoDir).invariantSeparatorsPath }
                .forEach { f ->
                    digest.update(f.relativeTo(repoDir).invariantSeparatorsPath.toByteArray())
                    digest.update(f.readBytes())
                }
            listOf("build.gradle.kts", "settings.gradle.kts").map(repoDir::resolve).filter { it.isFile }
                .forEach { digest.update(it.readBytes()) }
            "-dev" + digest.digest().take(4).joinToString("") { b -> "%02x".format(b) }
        } else {
            ""
        }
        val versionText = "$serverVersion-$gitCommit$sourceHash"
        File(dir, "server-version.txt").writeText(versionText)
    }
}

sourceSets {
    main {
        resources.srcDir(generateServerVersion)
    }
}
