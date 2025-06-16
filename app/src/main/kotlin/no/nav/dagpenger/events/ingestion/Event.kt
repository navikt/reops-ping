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
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.UUID

data class Event(
    val hendelsesNavn: String,
    val attributes: Map<String, Any>,
    val json: String,
    val appEier: String,
    val appNavn: String,
    val appMiljo: String,
    val urlDomene: String? = null,
    val urlSti: String? = null,
    val urlParametre: String? = null,
) {
    val uuid: UUID = UuidCreator.getTimeOrderedEpoch()
    val createdAt: Instant = ZonedDateTime.now(OSLO_ZONE).toInstant()

    companion object {
        private val OSLO_ZONE = ZoneId.of("Europe/Oslo")

        fun fraJson(json: String): Event =
            try {
                val parsed = Json.parseToJsonElement(json).jsonObject

                val hendelsesNavn =
                    parsed["hendelse_navn"]
                        ?.jsonPrimitive
                        ?.contentOrNull
                        ?: parsed["event_name"]
                            ?.jsonPrimitive
                            ?.contentOrNull
                if (hendelsesNavn.isNullOrBlank()) throw IllegalArgumentException("Missing 'hendelse_navn'")

                val appEier =
                    parsed["app_eier"]
                        ?.jsonPrimitive
                        ?.contentOrNull
                        ?: parsed["team"]
                            ?.jsonPrimitive
                            ?.contentOrNull
                        ?: throw IllegalArgumentException("Missing 'app_eier'")
                val appNavn =
                    parsed["app_navn"]
                        ?.jsonPrimitive
                        ?.contentOrNull
                        ?: parsed["app"]
                            ?.jsonPrimitive
                            ?.contentOrNull
                        ?: throw IllegalArgumentException("Missing 'app_navn'")
                val appMiljo =
                    parsed["app_miljo"]
                        ?.jsonPrimitive
                        ?.contentOrNull
                        ?: parsed["environment"]
                            ?.jsonPrimitive
                            ?.contentOrNull
                        ?: throw IllegalArgumentException("Missing 'app_miljo'")

                // Extract optional URL fields
                val urlDomene =
                    parsed["url_domene"]
                        ?.jsonPrimitive
                        ?.contentOrNull
                        ?: parsed["url_host"]
                            ?.jsonPrimitive
                            ?.contentOrNull
                val urlSti =
                    parsed["url_sti"]
                        ?.jsonPrimitive
                        ?.contentOrNull
                        ?: parsed["url_path"]
                            ?.jsonPrimitive
                            ?.contentOrNull
                val urlParametre =
                    parsed["url_parametre"]
                        ?.jsonPrimitive
                        ?.contentOrNull
                        ?: parsed["url_query"]
                            ?.jsonPrimitive
                            ?.contentOrNull

                // Get attributes directly from the JSON object, not from a nested "payload" field
                val attributes = flatJsonToMap(parsed)

                Event(
                    hendelsesNavn,
                    attributes,
                    json,
                    appEier,
                    appNavn,
                    appMiljo,
                    urlDomene,
                    urlSti,
                    urlParametre,
                )
            } catch (e: Exception) {
                throw IllegalArgumentException("Malformed JSON: ${e.message}")
            }

        private fun flatJsonToMap(json: JsonElement): Map<String, Any> {
            require(json is JsonObject) { "JSON må være et objekt på toppnivå." }

            // Filter out the fields we've already processed
            val reservedFields =
                setOf(
                    "hendelse_navn", "event_name",
                    "app_eier", "team",
                    "app_navn", "app",
                    "app_miljo", "environment",
                    "url_domene", "url_host",
                    "url_sti", "url_path",
                    "url_parametre", "url_query",
                )

            return json.entries
                .filter { (key, _) -> key !in reservedFields }
                .associate { (key, value) ->
                    key to
                        when (value) {
                            is JsonPrimitive -> {
                                when {
                                    value.isString -> value.content
                                    value.booleanOrNull != null -> value.boolean
                                    value.longOrNull != null -> value.long
                                    value.doubleOrNull != null -> value.double
                                    else -> value.content // fallback
                                }
                            }
                            else -> value.toString() // serialize arrays/objects as string
                        }
                }
        }
    }
}
