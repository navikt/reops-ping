package no.nav.dagpenger.events.duckdb

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration

class PeriodicTrigger(
    private val batchSize: Int,
    private val interval: Duration,
) {
    private var action: () -> Unit = {}
    private val counter = AtomicInteger(0)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    fun start() {
        scope.launch {
            while (isActive) {
                delay(interval)
                if (counter.get() >= 1) {
                    action()
                }
            }
        }
    }

    fun stop() {
        // Kjør action en siste gang før stopp
        scope.launch {
            action()
        }
        scope.cancel()
    }

    fun increment(by: Int = 1) {
        val newValue = counter.addAndGet(by)
        if (newValue >= batchSize) {
            // Kjører action med en gang
            scope.launch {
                action()
            }
            // Nullstill telleren
            counter.set(0)
        }
    }

    fun register(block: () -> Unit): PeriodicTrigger {
        this.action = block
        return this
    }
}
