package no.nav.dagpenger.events.duckdb

import mu.KotlinLogging
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class DuckDbStore(
    private val conn: Connection,
    private val periodicTrigger: PeriodicTrigger,
    gcsBucketPrefix: String,
) {
    init {
        conn.createStatement().use {
            it.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS events (
                    ts TIMESTAMP,
                    event_name TEXT,
                    payload JSON
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
        val timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now()).replace(":", "-")
        val path = hivePath(LocalDateTime.now())
        val gcsPath = "$bucketPathPrefix/$path.parquet"

        logger.info { "Flushing events to $gcsPath" }

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
            stmt.executeUpdate("COPY to_export TO '$gcsPath' (FORMAT 'parquet')")
            stmt.executeUpdate("DROP TABLE to_export")
        }
    }

    private val zoneOffset = ZoneOffset.UTC

    private fun hivePath(now: LocalDateTime = LocalDateTime.now()): String =
        "year=${now.year}/month=${now.month.value}/day=${now.dayOfMonth}/${now.toEpochSecond(zoneOffset)}"

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
