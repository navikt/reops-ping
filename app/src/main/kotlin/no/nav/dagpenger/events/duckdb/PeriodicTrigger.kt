package no.nav.dagpenger.events.duckdb

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class PeriodicTrigger(
    private val batchSize: Int,
    private val interval: Duration,
) {
    private var action: () -> Unit = {}
    private val counter = AtomicInteger(0)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val mutex = Mutex()

    fun start() {
        scope.launch {
            while (isActive) {
                delay(interval)
                if (counter.get() >= 1) {
                    flushSafely()
                }
            }
        }
    }

    fun stop() {
        // Kjør action en siste gang før stopp
        scope.launch {
            if (counter.get() >= 1) {
                flushSafely()
            }
        }
        scope.cancel()
    }

    fun increment(by: Int = 1) {
        val newValue = counter.addAndGet(by)
        if (newValue >= batchSize) {
            // Kjører action med en gang
            scope.launch {
                flushSafely()
            }
            // Nullstill telleren
            counter.set(0)
        }
    }

    private suspend fun flushSafely() {
        withTimeout(60.seconds) {
            mutex.withLock {
                try {
                    action()
                } finally {
                    counter.set(0)
                }
            }
        }
    }

    fun register(block: () -> Unit): PeriodicTrigger {
        this.action = block
        return this
    }
}
