package no.nav.dagpenger.events.duckdb

import com.google.cloud.storage.Storage
import io.kotest.matchers.shouldBe
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import no.nav.dagpenger.events.ingestion.Event
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.sql.DriverManager
import java.sql.Timestamp

class DuckDbStoreTest {
    private val connection = DriverManager.getConnection("jdbc:duckdb:")
    private val storage = mockk<Storage>(relaxed = true)
    private val duckDbStore = DuckDbStore(connection, "gs://test-bucket/event", "gs://test-bucket/attribute", storage)
    private val periodicTrigger: TestTrigger =
        TestTrigger {
            runBlocking { duckDbStore.flushToParquetAndClear() }
        }.also { duckDbStore.addObserver(it) }

    @Test
    fun `insertEvent should insert event into database`() {
        val eventName = "some_event"
        val attributes =
            mapOf(
                "string" to "value",
                "boolean" to true,
                "number" to 42.0,
            )
        val json = """{"hendelse_navn": "$eventName", "app_eier": "team", "app_navn": "app", "app_miljo": "env", "string": "value"}"""
        val event = Event(eventName, attributes, json, "team", "app", "env")

        runBlocking { duckDbStore.insertEvent(event) }

        connection.prepareStatement("SELECT * FROM event").use {
            val rs = it.executeQuery()
            while (rs.next()) {
                rs.getString(1) shouldBe event.uuid.toString()
                rs.getTimestamp(2) shouldBe Timestamp.from(event.createdAt)
                rs.getString(3) shouldBe event.appEier
                rs.getString(4) shouldBe event.appNavn
                rs.getString(5) shouldBe event.appMiljo
                rs.getString(6) shouldBe event.hendelsesNavn
            }
        }

        periodicTrigger.counter shouldBe 1
        periodicTrigger.trigger()

        verify(exactly = 2) {
            storage.createFrom(any(), any<Path>())
        }
    }

    private class TestTrigger(
        var action: suspend () -> Unit = {},
    ) : DuckDbObserver {
        var counter: Int = 0

        fun trigger() {
            runBlocking { action() }
        }

        override fun onInsert() {
            counter++
        }
    }
}
