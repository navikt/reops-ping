package no.nav.reops.events.api

import kotlinx.serialization.Serializable

@Serializable
data class Problem(
    val type: String = "about:blank",
    val status: Int? = null,
    val title: String? = null,
    val detail: String? = null,
    val instance: String? = null,
    // val extensions: Map<String, Any>? = null,
)
