package no.nav.dagpenger.events.duckdb

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

class DuckDbStore(
    private val conn: Connection,
    private val periodicTrigger: PeriodicTrigger,
    gcsBucketPrefix: String,
    private val storage: Storage = StorageOptions.getDefaultInstance().service,
) {
    init {
        conn.createStatement().use {
            it.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS events (
                    ts TIMESTAMP,
                    event_name TEXT,
                    payload TEXT 
                )
                """.trimIndent(),
            )
        }

        periodicTrigger.register { flushToParquetAndClear(gcsBucketPrefix) }.start()
    }

    fun insertEvent(
        ts: Instant,
        eventName: String,
        payload: String,
    ) {
        conn.prepareStatement("INSERT INTO events (ts, event_name, payload) VALUES (?, ?, ?)").use { stmt ->
            stmt.setTimestamp(1, Timestamp.from(ts))
            stmt.setString(2, eventName)
            stmt.setString(3, payload)
            stmt.executeUpdate()
        }

        periodicTrigger.increment()
    }

    private fun flushToParquetAndClear(bucketPathPrefix: String) {
        val partition = hivePath(LocalDateTime.now())
        val gcsFile = "$bucketPathPrefix/$partition.parquet"
        val localFile = Files.createTempFile("events-", ".parquet")

        logger.info { "Flushing events to $localFile" }

        conn.autoCommit = false
        try {
            conn.createStatement().use { stmt ->
                stmt.executeUpdate("CREATE TABLE to_export AS SELECT * FROM events")
                stmt.executeUpdate("DELETE FROM events")
            }
            conn.commit()
        } catch (e: Exception) {
            conn.rollback()
            throw e
        }

        conn.createStatement().use { stmt ->
            stmt.executeUpdate("COPY to_export TO '$localFile' (FORMAT 'parquet')")
            stmt.executeUpdate("DROP TABLE to_export")
        }

        copyToBucket(gcsFile, localFile)
    }

    private fun copyToBucket(
        gcsPath: String,
        localFile: Path,
    ) {
        val blobId = BlobId.fromGsUtilUri(gcsPath)
        val blobInfo = BlobInfo.newBuilder(blobId).build()
        storage.createFrom(blobInfo, localFile)
    }

    private fun hivePath(now: LocalDateTime = LocalDateTime.now()) =
        "year=${now.year}/month=${now.month.value}/day=${now.dayOfMonth}/${now.toEpochSecond(ZoneOffset.UTC)}"

    companion object {
        private val logger = KotlinLogging.logger { }

        fun createInMemoryStore(
            gcsBucketPrefix: String,
            trigger: PeriodicTrigger,
        ) = DuckDbStore(
            DriverManager.getConnection("jdbc:duckdb:"),
            trigger,
            gcsBucketPrefix,
        )
    }
}
