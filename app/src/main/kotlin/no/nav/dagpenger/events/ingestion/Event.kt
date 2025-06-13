package no.nav.dagpenger.events.ingestion

import com.github.f4b6a3.uuid.UuidCreator
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.double
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.longOrNull
import java.time.Instant
import java.util.UUID

data class Event(
    val eventName: String,
    val attributes: Map<String, Any>,
    val json: String,
    val team: String,
    val app: String,
    val environment: String,
    val urlHost: String? = null,
    val urlPath: String? = null,
    val urlQuery: String? = null,
) {
    val uuid: UUID = UuidCreator.getTimeOrderedEpoch()
    val createdAt: Instant = Instant.now()

    companion object {
        fun fraJson(json: String): Event =
            try {
                val parsed = Json.parseToJsonElement(json).jsonObject

                val eventName = parsed["event_name"]?.jsonPrimitive?.contentOrNull
                if (eventName.isNullOrBlank()) throw IllegalArgumentException("Missing 'event_name'")

                val team =
                    parsed["team"]?.jsonPrimitive?.contentOrNull
                        ?: throw IllegalArgumentException("Missing 'team'")
                val app =
                    parsed["app"]?.jsonPrimitive?.contentOrNull
                        ?: throw IllegalArgumentException("Missing 'app'")
                val environment =
                    parsed["environment"]?.jsonPrimitive?.contentOrNull
                        ?: throw IllegalArgumentException("Missing 'environment'")

                // Extract optional URL fields
                val urlHost = parsed["url_host"]?.jsonPrimitive?.contentOrNull
                val urlPath = parsed["url_path"]?.jsonPrimitive?.contentOrNull
                val urlQuery = parsed["url_query"]?.jsonPrimitive?.contentOrNull

                val attributes = parsed["payload"]?.let { flatJsonToMap(it) } ?: emptyMap()

                Event(
                    eventName,
                    attributes,
                    json,
                    team,
                    app,
                    environment,
                    urlHost,
                    urlPath,
                    urlQuery,
                )
            } catch (e: Exception) {
                throw IllegalArgumentException("Malformed JSON: ${e.message}")
            }

        private fun flatJsonToMap(json: JsonElement): Map<String, Any> {
            require(json is JsonObject) { "JSON må være et objekt på toppnivå." }

            return json.mapValues { (_, value) ->
                when (value) {
                    is JsonPrimitive ->
                        when {
                            value.isString -> value.content
                            value.booleanOrNull != null -> value.boolean
                            value.longOrNull != null -> value.long
                            value.doubleOrNull != null -> value.double
                            else -> value.content // fallback
                        }

                    else -> value.toString() // serialiser arrays/objects som string
                }
            }
        }
    }
}
