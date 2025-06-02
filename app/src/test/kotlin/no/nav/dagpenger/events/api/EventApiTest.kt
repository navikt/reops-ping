package no.nav.dagpenger.events.api

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import no.nav.dagpenger.events.ingestion.EventIngestor
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Test

class EventApiTest {
    @Test
    fun `skal akseptere gyldig event via HTTP`() {
        testApplication {
            val ingestor = EventIngestorFake()
            application {
                eventApi(ingestor)
            }

            @Language("JSON")
            val eventJson = """{"event_name": "bar", "payload": {"some-data": "/"}}"""
            val response =
                client.post("/event") {
                    contentType(ContentType.Application.ProblemJson)
                    setBody(eventJson)
                }

            response.status shouldBe HttpStatusCode.Accepted

            ingestor.receivedEvents shouldHaveSize 1
            ingestor.receivedEvents.first() shouldBe eventJson
        }
    }

    private class EventIngestorFake : EventIngestor() {
        val receivedEvents = mutableListOf<String>()

        override fun storeEvent(
            eventName: String,
            json: String,
        ) {
            receivedEvents.add(json)
        }
    }
}
