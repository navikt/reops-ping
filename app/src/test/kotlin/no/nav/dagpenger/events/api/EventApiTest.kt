package no.nav.dagpenger.events.api

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import no.nav.dagpenger.events.ingestion.Event
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
            val eventJson =
                """
                {"hendelse_navn": "bar", "app_eier": "team", "app_navn": "app", 
                "app_miljo": "env", "some-data": "/"}
                """.trimIndent().replace("\n", "")

            val response =
                client.post("/event") {
                    contentType(ContentType.Application.Json)
                    setBody(eventJson)
                }

            response.status shouldBe HttpStatusCode.Accepted

            ingestor.receivedEvents shouldHaveSize 1
            ingestor.receivedEvents.first() shouldBe eventJson
        }
    }

    @Test
    fun `skal håndtere feil ved skriving av event via HTTP`() {
        testApplication {
            val ingestor = EventIngestorFake(true)
            application {
                eventApi(ingestor)
            }

            @Language("JSON")
            val eventJson =
                """
                {"hendelse_navn": "bar", "app_eier": "team", "app_navn": "app", 
                "app_miljo": "env", "some-data": "/"}
                """.trimIndent().replace("\n", "")

            val response =
                client.post("/event") {
                    contentType(ContentType.Application.Json)
                    setBody(eventJson)
                }

            response.status shouldBe HttpStatusCode.InternalServerError
        }
    }

    private class EventIngestorFake(
        val flushing: Boolean = false,
    ) : EventIngestor() {
        val receivedEvents = mutableListOf<String>()

        override suspend fun storeEvent(event: Event) {
            if (flushing) {
                throw IllegalStateException("Flushing does not allow writes")
            }
            receivedEvents.add(event.json)
        }
    }
}
