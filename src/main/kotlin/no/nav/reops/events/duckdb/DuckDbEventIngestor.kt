package no.nav.reops.events.duckdb

import no.nav.reops.events.ingestion.Event
import no.nav.reops.events.ingestion.EventIngestor

class DuckDbEventIngestor(
    private val store: DuckDbStore,
) : EventIngestor() {
    override suspend fun storeEvent(event: Event) {
        store.insertEvent(event)
    }
}
