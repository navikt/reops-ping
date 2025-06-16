package no.nav.dagpenger.events.duckdb

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import no.nav.dagpenger.events.ingestion.Event
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID

fun interface DuckDbObserver {
    fun onInsert()
}

class DuckDbStore internal constructor(
    private val conn: Connection,
    private val gcsBucketEvent: String,
    private val gcsBucketAttribute: String,
    private val storage: Storage,
) {
    constructor(
        gcsBucketPrefixEvent: String,
        gcsBucketPrefixAttribute: String,
    ) : this(
        DriverManager.getConnection("jdbc:duckdb:"),
        gcsBucketPrefixEvent,
        gcsBucketPrefixAttribute,
        StorageOptions.getDefaultInstance().service,
    )

    private val observers = mutableListOf<DuckDbObserver>()
    private val mutex = Mutex()

    init {
        conn.createStatement().use {
            //language=PostgreSQL
            it.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS event
                (
                    hendelse_id        TEXT PRIMARY KEY,
                    hendelse_tidspunkt TIMESTAMP,
                    app_eier           TEXT,
                    app_navn           TEXT,
                    app_miljo          TEXT,
                    hendelse_navn      TEXT,
                    payload            TEXT,
                    url_domene         TEXT,
                    url_sti            TEXT,
                    url_parametre      TEXT
                );
                
                CREATE TABLE IF NOT EXISTS event_attribute
                (
                    hendelse_id      TEXT,
                    hendelse_navn    TEXT,
                    key              TEXT,
                    type             TEXT,
                    value_string     TEXT,
                    value_bool       BOOLEAN,
                    value_number     DOUBLE,
                    hendelse_tidspunkt TIMESTAMP
                );
                """.trimIndent(),
            )
        }
    }

    fun addObserver(observer: DuckDbObserver) {
        observers.add(observer)
    }

    suspend fun insertEvent(event: Event) {
        mutex.withLock {
            conn.autoCommit = false
            try {
                conn
                    .prepareStatement(
                        """INSERT INTO event (
                            hendelse_id, hendelse_tidspunkt, app_eier, app_navn, app_miljo, hendelse_navn, payload, 
                            url_domene, url_sti, url_parametre
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    )
                    .use { stmt ->
                        stmt.setString(1, event.uuid.toString())
                        stmt.setTimestamp(2, Timestamp.from(event.createdAt))
                        stmt.setString(3, event.appEier)
                        stmt.setString(4, event.appNavn)
                        stmt.setString(5, event.appMiljo)
                        stmt.setString(6, event.hendelsesNavn)
                        stmt.setString(7, event.json)

                        if (event.urlDomene != null) {
                            stmt.setString(8, event.urlDomene)
                        } else {
                            stmt.setNull(8, java.sql.Types.VARCHAR)
                        }

                        if (event.urlSti != null) {
                            stmt.setString(9, event.urlSti)
                        } else {
                            stmt.setNull(9, java.sql.Types.VARCHAR)
                        }

                        if (event.urlParametre != null) {
                            stmt.setString(10, event.urlParametre)
                        } else {
                            stmt.setNull(10, java.sql.Types.VARCHAR)
                        }

                        stmt.executeUpdate()
                    }

                event.attributes.forEach { (key, value) ->
                    conn
                        .prepareStatement(
                            //language=PostgreSQL
                            """
                            INSERT INTO event_attribute (
                                hendelse_id, 
                                hendelse_navn, 
                                key, 
                                type, 
                                value_string, 
                                value_bool, 
                                value_number, 
                                hendelse_tidspunkt
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """.trimIndent(),
                        ).use { stmt ->
                            stmt.setString(1, event.uuid.toString())
                            stmt.setString(2, event.hendelsesNavn)
                            stmt.setString(3, key)
                            when (value) {
                                is String -> {
                                    stmt.setString(4, "string")
                                    stmt.setString(5, value)
                                    stmt.setNull(6, java.sql.Types.BOOLEAN)
                                    stmt.setNull(7, java.sql.Types.DOUBLE)
                                }

                                is Boolean -> {
                                    stmt.setString(4, "boolean")
                                    stmt.setNull(5, java.sql.Types.VARCHAR)
                                    stmt.setBoolean(6, value)
                                    stmt.setNull(7, java.sql.Types.DOUBLE)
                                }

                                is Long -> {
                                    stmt.setString(4, "double")
                                    stmt.setNull(5, java.sql.Types.VARCHAR)
                                    stmt.setNull(6, java.sql.Types.BOOLEAN)
                                    stmt.setDouble(7, value.toDouble())
                                }

                                is Double -> {
                                    stmt.setString(4, "double")
                                    stmt.setNull(5, java.sql.Types.VARCHAR)
                                    stmt.setNull(6, java.sql.Types.BOOLEAN)
                                    stmt.setDouble(7, value)
                                }

                                else -> {
                                    throw IllegalArgumentException(
                                        "Unsupported attribute type: " +
                                            "${value::class.java}",
                                    )
                                }
                            }
                            stmt.setTimestamp(8, Timestamp.from(event.createdAt))
                            stmt.executeUpdate()
                        }
                }
                conn.commit()

                observers.emit { onInsert() }
            } catch (e: Exception) {
                conn.rollback()
                throw e
            }
        }
    }

    suspend fun flushToParquetAndClear() =
        withContext(Dispatchers.IO) {
            mutex.withLock {
                flushTable("event", gcsBucketEvent)

                // Only flush event_attribute table if it has records
                if (tableHasRecords("event_attribute")) {
                    flushTable("event_attribute", gcsBucketAttribute)
                } else {
                    logger.info { "Skipping event_attribute flush as table is empty" }
                    // Still need to clear the table even if empty
                    clearTable("event_attribute")
                }

                logger.info { "Flush finished" }
            }
        }

    private fun tableHasRecords(table: String): Boolean {
        return conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT COUNT(*) FROM $table").use { rs ->
                rs.next() && rs.getLong(1) > 0
            }
        }
    }

    private fun clearTable(table: String) {
        conn.createStatement().use { stmt ->
            stmt.executeUpdate("DELETE FROM $table")
        }
    }

    private fun flushTable(
        table: String,
        gcsBucketPrefix: String,
    ) {
        val gcsFile = "$gcsBucketPrefix/${partitionPath()}.parquet"
        val localFile = Files.createTempFile("events-", ".parquet")
        val exportTable = "export_$table"

        logger.info { "Making copy of table=$table to flush" }

        conn.autoCommit = false
        try {
            conn.createStatement().use { stmt ->
                stmt.executeUpdate("CREATE TABLE $exportTable AS SELECT * FROM $table")
                stmt.executeUpdate("DELETE FROM $table")
            }
            conn.commit()
        } catch (e: Exception) {
            conn.rollback()
            throw e
        }

        logger.info { "Exporting $table to $localFile" }

        conn.createStatement().use { stmt ->
            stmt.executeUpdate("COPY $exportTable TO '$localFile' (FORMAT 'parquet')")
            stmt.executeUpdate("DROP TABLE $exportTable")
        }

        logger.info { "Copying Parquet-file to $gcsFile" }
        copyToBucket(localFile, gcsFile)
    }

    private fun copyToBucket(
        localFile: Path,
        gcsPath: String,
    ) {
        val blobId = BlobId.fromGsUtilUri(gcsPath)
        val blobInfo = BlobInfo.newBuilder(blobId).build()
        storage.createFrom(blobInfo, localFile)
    }

    private fun partitionPath(now: LocalDateTime = LocalDateTime.now()): String {
        return "${UUID.randomUUID()}"
    }

    companion object {
        private val logger = KotlinLogging.logger { }

        private fun Iterable<DuckDbObserver>.emit(block: DuckDbObserver.() -> Unit) = forEach { observer -> observer.block() }
    }
}
