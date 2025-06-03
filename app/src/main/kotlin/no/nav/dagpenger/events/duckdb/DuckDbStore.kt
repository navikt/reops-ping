package no.nav.dagpenger.events.duckdb

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDateTime
import java.util.UUID

class DuckDbStore(
    private val conn: Connection,
    private val periodicTrigger: PeriodicTrigger,
    gcsBucketPrefix: String,
    private val storage: Storage = StorageOptions.getDefaultInstance().service
) {
    init {
        logger.info { "Initializing DuckDbStore with gcsBucketPrefix: $gcsBucketPrefix" }
        try {
            conn.createStatement().use {
                logger.debug { "Creating events table if it doesn't exist" }
                // language=GenericSQL
                it.executeUpdate(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                        ts TIMESTAMP,
                        event_name TEXT,
                        payload TEXT,
                        collected_by TEXT
                    )
                    """.trimIndent()
                )
                logger.debug { "Events table created or already exists" }
            }

            logger.info { "Registering periodic trigger for flushing events" }
            periodicTrigger.register { flushToParquetAndClear(gcsBucketPrefix) }.start()
            logger.info { "DuckDbStore initialization complete" }
        } catch (e: Exception) {
            logger.error(e) { "Failed to initialize DuckDbStore" }
            throw e
        }
    }

    private val appName by lazy {
        System.getenv("NAIS_APP_NAME") ?: "unknown-app"
    }

    fun insertEvent(
        ts: Instant,
        eventName: String,
        payload: String
    ) {
        logger.debug { "Inserting event: name=$eventName, timestamp=$ts" }
        try {
            conn.prepareStatement("INSERT INTO events (ts, event_name, payload, collected_by) VALUES (?, ?, ?, ?)")
                .use { stmt ->
                    stmt.setTimestamp(1, Timestamp.from(ts))
                    stmt.setString(2, eventName)
                    stmt.setString(3, payload)
                    stmt.setString(4, appName)
                    val rowsAffected = stmt.executeUpdate()
                    logger.debug { "Successfully inserted event: $eventName, rows affected: $rowsAffected" }
                }

            logger.debug { "Incrementing event counter in trigger" }
            periodicTrigger.increment()
            logger.debug { "Event successfully processed and counter incremented" }
        } catch (e: Exception) {
            logger.error(e) { "Failed to insert event: $eventName with payload: $payload" }
            throw e
        }
    }

    private suspend fun flushToParquetAndClear(bucketPathPrefix: String) = withContext(Dispatchers.IO) {
        logger.info { "Starting flush process to export events to Parquet" }
        val partition = hivePath(LocalDateTime.now())
        val gcsFile = "$bucketPathPrefix/$partition.parquet"
        val localFile = Files.createTempFile("events-", ".parquet")
        logger.debug { "Created temp file: $localFile" }

        logger.info { "Making copy of events-table to flush, creating to_export table" }

        conn.autoCommit = false
        try {
            conn.createStatement().use { stmt ->
                logger.debug { "Creating to_export table from events" }
                val rowsCopied = conn.prepareStatement("SELECT COUNT(*) FROM events").use {
                    it.executeQuery().use { rs ->
                        if (rs.next()) rs.getInt(1) else 0
                    }
                }
                logger.info { "Preparing to export $rowsCopied events" }
                
                stmt.executeUpdate("CREATE TABLE to_export AS SELECT * FROM events")
                logger.debug { "Deleting records from events table" }
                stmt.executeUpdate("DELETE FROM events")
            }
            conn.commit()
            logger.debug { "Transaction committed" }
        } catch (e: Exception) {
            logger.error(e) { "Error during table copy, rolling back transaction" }
            conn.rollback()
            throw e
        }

        logger.info { "Exporting events to Parquet file: $localFile" }

        try {
            conn.createStatement().use { stmt ->
                stmt.executeUpdate("COPY to_export TO '$localFile' (FORMAT 'parquet')")
                logger.debug { "Successfully wrote data to Parquet file" }
                stmt.executeUpdate("DROP TABLE to_export")
                logger.debug { "Dropped temporary to_export table" }
            }

            logger.info { "Copying Parquet-file to GCS: $gcsFile" }
            copyToBucket(gcsFile, localFile)

            // Get file size for logging
            val fileSize = Files.size(localFile) / 1024 // KB
            logger.info { "Flush finished successfully. File size: $fileSize KB" }
            
            try {
                logger.debug { "Deleting temporary file: $localFile" }
                Files.deleteIfExists(localFile)
            } catch (e: Exception) {
                logger.warn(e) { "Failed to delete temporary file: $localFile" }
            }
        } catch (e: Exception) {
            logger.error(e) { "Failed to export data to Parquet or upload to GCS" }
            throw e
        }
    }

    private fun copyToBucket(
        gcsPath: String,
        localFile: Path
    ) {
        logger.debug { "Starting upload to GCS: $gcsPath" }
        try {
            val blobId = BlobId.fromGsUtilUri(gcsPath)
            logger.debug { "Created BlobId: $blobId" }
            
            val blobInfo = BlobInfo.newBuilder(blobId).build()
            logger.debug { "Created BlobInfo: $blobInfo" }
            
            val startTime = System.currentTimeMillis()
            storage.createFrom(blobInfo, localFile)
            val duration = System.currentTimeMillis() - startTime
            
            logger.info { "Upload to GCS completed successfully. Path: $gcsPath, Duration: $duration ms" }
        } catch (e: Exception) {
            logger.error(e) { "Failed to upload file to GCS. Path: $gcsPath, Local file: $localFile" }
            throw e
        }
    }

    private fun hivePath(now: LocalDateTime = LocalDateTime.now()): String {
        val path = "year=${now.year}/month=${now.month.value}/day=${now.dayOfMonth}/${UUID.randomUUID()}"
        logger.debug { "Generated Hive path: $path" }
        return path
    }

    companion object {
        private val logger = KotlinLogging.logger { }

        fun createInMemoryStore(
            gcsBucketPrefix: String,
            trigger: PeriodicTrigger
        ): DuckDbStore {
            logger.info { "Creating in-memory DuckDB store with bucket prefix: $gcsBucketPrefix" }
            try {
                logger.debug { "Opening DuckDB connection" }
                val connection = DriverManager.getConnection("jdbc:duckdb:")
                logger.debug { "Connection established successfully" }
                
                return DuckDbStore(
                    connection,
                    trigger,
                    gcsBucketPrefix
                ).also {
                    logger.info { "In-memory DuckDbStore created successfully" }
                }
            } catch (e: Exception) {
                logger.error(e) { "Failed to create in-memory DuckDbStore" }
                throw e
            }
        }
    }
}
