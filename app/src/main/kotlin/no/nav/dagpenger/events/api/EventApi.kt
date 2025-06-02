package no.nav.dagpenger.events.api

import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.plugins.calllogging.CallLogging
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.request.path
import io.ktor.server.request.receiveText
import io.ktor.server.request.uri
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import mu.KotlinLogging
import no.nav.dagpenger.events.duckdb.PeriodicTrigger
import no.nav.dagpenger.events.ingestion.EventIngestor
import org.slf4j.event.Level

private val callLogger = KotlinLogging.logger("CallLogging")

private val ignoredPaths =
    setOf(
        "/internal/isalive",
        "internal/isready",
    )

fun Application.eventApi(
    ingestor: EventIngestor,
    trigger: PeriodicTrigger? = null,
) {
    installStatusPages()
    install(CallLogging) {
        level = Level.INFO
        logger = callLogger
        disableDefaultColors()

        filter { call ->
            ignoredPaths.none { ignoredPath ->
                call.request.path().startsWith(ignoredPath)
            }
        }
    }

    routing {
        route("/internal") {
            get("/isalive") { call.respond(HttpStatusCode.OK, "OK") }
            get("/isready") { call.respond(HttpStatusCode.OK, "OK") }
            get("/stop") {
                trigger?.stop()
                call.respond(HttpStatusCode.OK, "Stopping server")
            }
        }

        post("/event") {
            val body = call.receiveText()
            ingestor.handleEvent(body)
            call.respond(HttpStatusCode.Accepted)
        }
    }
}

private fun Application.installStatusPages() {
    install(StatusPages) {
        exception<Throwable> { call, cause ->
            val httpStatusCode = HttpStatusCode.InternalServerError
            val problem =
                Problem(
                    type = "internal-server-error.html",
                    status = httpStatusCode.value,
                    title = httpStatusCode.description,
                    detail = cause.message,
                    instance = call.request.uri,
                )
            call.respond(httpStatusCode, problem)
        }
    }
}
