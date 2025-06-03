package no.nav.dagpenger.events

import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import mu.KotlinLogging
import no.nav.dagpenger.events.api.eventApi
import no.nav.dagpenger.events.duckdb.DuckDbEventIngestor
import no.nav.dagpenger.events.duckdb.DuckDbStore
import no.nav.dagpenger.events.duckdb.PeriodicTrigger
import kotlin.time.Duration.Companion.seconds

private val logger = KotlinLogging.logger {}

fun main() {
    logger.info { "Starting application with debug logging enabled" }

    val batchSize = System.getenv("GCS_BATCH_SIZE")?.toIntOrNull() ?: 1000
    val maxInterval = System.getenv("GCS_MAX_INTERVAL_SECONDS")?.toIntOrNull() ?: 10
    val gcsBucketPrefix =
        System.getenv("GCS_BUCKET_PREFIX")
            ?: throw IllegalArgumentException("Environment variable GCS_BUCKET_PREFIX is not set")

    logger.info { "Configuration: batchSize=$batchSize, maxInterval=$maxInterval seconds, gcsBucketPrefix=$gcsBucketPrefix" }

    logger.info { "Initializing PeriodicTrigger with batchSize=$batchSize and interval=${maxInterval}s" }
    val trigger =
        PeriodicTrigger(batchSize, maxInterval.seconds).apply {
            logger.info { "Setting up shutdown hook for PeriodicTrigger" }
            Runtime.getRuntime().addShutdownHook(
                Thread {
                    logger.info("Shutdown hook triggered. Cleaning up...")
                    stop()
                    logger.info("Cleanup complete.")
                },
            )
        }

    logger.info { "Creating DuckDbStore with bucket prefix: $gcsBucketPrefix" }
    val store = DuckDbStore.createInMemoryStore(gcsBucketPrefix, trigger)

    logger.info { "Initializing DuckDbEventIngestor" }
    val ingestor = DuckDbEventIngestor(store)
    ingestor.also { logger.debug { "DuckDbEventIngestor instance: $it" } }

    embeddedServer(CIO, port = 8080) {
        logger.info { "Configuring event API endpoints" }
        eventApi(
            object : DuckDbEventIngestor(store) {
                override fun ingest(eventName: String, payload: String) {
                    logger.info { "Event received - name: $eventName, payload: $payload" }
                    try {
                        super.ingest(eventName, payload)
                        logger.info { "Event successfully ingested - name: $eventName" }
                    } catch (e: Exception) {
                        logger.error(e) { "Error ingesting event - name: $eventName" }
                        throw e
                    }
                }
            }
        )
    }.apply {
        logger.info { "Starting server on port 8080" }
        start(wait = true)
    }
}
