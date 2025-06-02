package no.nav.dagpenger.events.duckdb

import com.google.cloud.storage.Storage
import io.mockk.MockKSettings.relaxed
import io.mockk.Runs
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Statement
import java.sql.Timestamp
import java.time.Instant

class DuckDbStoreTest {
    private val connection: Connection = spyk(DriverManager.getConnection("jdbc:duckdb:"))
    private val mockPeriodicTrigger: PeriodicTrigger = mockk(relaxed = true)
    private val storage = mockk<Storage>(relaxed = true)

    private val duckDbStore =
        DuckDbStore(connection, mockPeriodicTrigger, "test-bucket", storage)

    @AfterEach
    fun tearDown() {
        clearMocks(connection, mockPeriodicTrigger)
    }

    @Test
    fun `insertEvent should insert event into database`() {
        val mockPreparedStatement: PreparedStatement = mockk(relaxed = true)
        every { connection.prepareStatement(any()) } returns mockPreparedStatement

        val timestamp = Instant.now()
        val eventName = "some_event"
        val payload = "{\"key\": \"value\"}"

        duckDbStore.insertEvent(timestamp, eventName, payload)

        verify(exactly = 1) {
            connection.prepareStatement("INSERT INTO events (ts, event_name, payload) VALUES (?, ?, ?)")
            mockPreparedStatement.setTimestamp(1, Timestamp.from(timestamp))
            mockPreparedStatement.setString(2, eventName)
            mockPreparedStatement.setString(3, payload)
            mockPreparedStatement.executeUpdate()
        }
        verify { mockPeriodicTrigger.increment() }
    }

    @Test
    fun `flushToParquetAndClear should flush events to Parquet and clear the database`() {
        val mockStatement: Statement = mockk(relaxed = true)
        every { connection.createStatement() } returns mockStatement
        every { connection.autoCommit = any() } just Runs

        invokePrivateFlush(duckDbStore, "gs://test-bucket")

        verifyOrder {
            connection.autoCommit = false
            connection.createStatement()
            mockStatement.executeUpdate("CREATE TABLE to_export AS SELECT * FROM events")
            mockStatement.executeUpdate("DELETE FROM events")
            connection.commit()
            // TODO: Trigger må kjøre
            // mockStatement.executeUpdate("COPY to_export TO 'test-bucket/events_2025-06-02T00-00-00Z.parquet' (FORMAT 'parquet')")
            // mockStatement.executeUpdate("DROP TABLE to_export")
        }
    }

    private fun invokePrivateFlush(
        duckDbStore: DuckDbStore,
        bucketPathPrefix: String,
    ) {
        val flushMethod =
            duckDbStore.javaClass
                .getDeclaredMethod(
                    "flushToParquetAndClear",
                    String::class.java,
                ).apply { isAccessible = true }
        flushMethod.invoke(duckDbStore, bucketPathPrefix)
    }
}
