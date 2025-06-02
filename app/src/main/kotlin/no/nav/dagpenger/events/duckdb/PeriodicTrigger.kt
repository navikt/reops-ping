package no.nav.dagpenger.events.duckdb

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class PeriodicTrigger(
    private val batchSize: Int,
    private val interval: Duration,
) {
    private var action: suspend () -> Unit = {}
    private val counter = AtomicInteger(0)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val mutex = Mutex()
    private var flushJob: Job? = null

    fun start() {
        scheduleIntervalFlush()
    }

    fun stop() {
        flushJob?.cancel()
        scope.launch {
            flushSafely()
        }
        scope.cancel()
    }

    fun increment(by: Int = 1) {
        val newValue = counter.addAndGet(by)
        if (newValue >= batchSize) {
            scope.launch {
                flushSafely()
            }
        }
    }

    private fun scheduleIntervalFlush() {
        flushJob?.cancel() // Cancel any previous timer
        flushJob =
            scope.launch {
                delay(interval)
                flushSafely()
            }
    }

    private suspend fun flushSafely() {
        if (!mutex.tryLock()) return // Avoid overlapping flushes
        try {
            withTimeout(60.seconds) {
                action()
                counter.set(0)
            }
        } finally {
            mutex.unlock()
            scheduleIntervalFlush() // Reschedule after successful flush
        }
    }

    fun register(block: suspend () -> Unit): PeriodicTrigger {
        this.action = block
        return this
    }
}
