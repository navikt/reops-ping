package no.nav.dagpenger.events.ingestion

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

abstract class EventIngestor {
    abstract fun storeEvent(
        eventName: String,
        json: String,
    )

    fun handleEvent(json: String) {
        val eventName =
            try {
                val parsed = Json.parseToJsonElement(json).jsonObject

                val eventName = parsed["event_name"]?.jsonPrimitive?.contentOrNull
                if (eventName.isNullOrBlank()) throw IllegalArgumentException("Missing 'event_name'")

                eventName
            } catch (e: Exception) {
                throw IllegalArgumentException("Malformed JSON: ${e.message}")
            }

        storeEvent(eventName, json)
    }
}
