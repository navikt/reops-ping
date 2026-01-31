plugins {
    application
    id("org.jetbrains.kotlin.jvm") version "2.2.21"
    id("org.jetbrains.kotlin.plugin.serialization") version "2.2.21"
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

kotlin {
    jvmToolchain(21)
}

dependencies {
    implementation(platform("io.ktor:ktor-bom:3.0.3"))
    implementation("io.ktor:ktor-server-core")
    implementation("io.ktor:ktor-server-netty")
    implementation("io.ktor:ktor-server-content-negotiation")
    implementation("io.ktor:ktor-serialization-kotlinx-json")
    implementation("io.ktor:ktor-server-cors")
    implementation("io.ktor:ktor-server-cio")
    implementation("io.ktor:ktor-server-call-logging")
    implementation("io.ktor:ktor-server-status-pages")

    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")

    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
    implementation("ch.qos.logback:logback-classic:1.5.26")
    implementation("net.logstash.logback:logstash-logback-encoder:9.0")

    implementation("com.github.f4b6a3:uuid-creator:6.1.1")
    implementation("com.google.cloud:google-cloud-storage:2.48.1")
    implementation("org.duckdb:duckdb_jdbc:1.1.3")

    // Override vulnerable transitive dependencies
    implementation("com.google.protobuf:protobuf-java:3.25.5")

    testImplementation(kotlin("test"))
    testImplementation("io.kotest:kotest-assertions-core:5.7.2")
    testImplementation("io.ktor:ktor-server-test-host")
    testImplementation("io.mockk:mockk:1.13.12")
}

application {
    mainClass = "no.nav.reops.events.AppKt"
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        showExceptions = true
        showStackTraces = true
    }
}