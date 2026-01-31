package no.nav.reops.events.duckdb

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

fun interface TriggerAction {
    suspend fun invoke()
}

enum class BackpressureStrategy {
    DROP, // Drop new events when capacity is reached
    BLOCK, // Block until capacity is available
    FORCE_FLUSH, // Force immediate flush when capacity is reached
}

// Primary constructor with all parameters
class PeriodicTrigger(
    private val batchSize: Int,
    private val interval: Duration,
    private val action: TriggerAction,
    private val maxCapacity: Int = batchSize * 3,
    private val backpressureStrategy: BackpressureStrategy = BackpressureStrategy.FORCE_FLUSH,
) : DuckDbObserver {
    // Constructor overload for backward compatibility
    constructor(
        batchSize: Int,
        interval: Duration,
        action: TriggerAction,
    ) : this(batchSize, interval, action, batchSize * 3, BackpressureStrategy.FORCE_FLUSH)

    // Constructor overload that accepts a regular suspend function
    constructor(
        batchSize: Int,
        interval: Duration,
        action: suspend () -> Unit,
    ) : this(batchSize, interval, TriggerAction { action() })

    private val counter = AtomicInteger(0)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private var flushJob: Job? = null
    private val backpressureActive = AtomicBoolean(false)
    private val flushing = AtomicBoolean(false)
    private var droppedEvents = AtomicInteger(0)

    internal fun start() {
        logger.info { "Starter å regelmessig flushe events som er færre en batch-størrelse" }
        scheduleIntervalFlush()
    }

    internal fun stop() {
        logger.info { "Avslutter regelmessig flushing" }
        flushJob?.cancel()
        if (counter.get() > 0) {
            runBlocking {
                flushSafely()
            }
        }
        scope.cancel()
    }

    private fun increment() {
        // Handle backpressure first
        handleBackpressure()

        // Original increment logic
        val newValue = counter.addAndGet(1)
        if (newValue >= batchSize) {
            // Atomically claim all current events for flushing
            val eventsToFlush = counter.getAndSet(0)
            if (eventsToFlush > 0) {
                logger.info { "Flusher data etter batch har nådd $eventsToFlush events" }
                scope.launch {
                    flushSafely()
                }
            }
        }
    }

    private fun handleBackpressure() {
        val currentCount = counter.get()

        // Check if we're about to exceed capacity
        if (currentCount >= maxCapacity) {
            backpressureActive.set(true)

            when (backpressureStrategy) {
                BackpressureStrategy.DROP -> {
                    // Drop the event and track it
                    droppedEvents.incrementAndGet()
                    logger.warn { "Backpressure applied: Event dropped (total dropped: ${droppedEvents.get()})" }
                    throw BackpressureException("Event dropped due to backpressure (DROP strategy)")
                }
                BackpressureStrategy.BLOCK -> {
                    // Block until there's capacity
                    while (counter.get() >= maxCapacity) {
                        logger.warn { "Backpressure applied: Blocking until capacity available" }
                        Thread.sleep(50) // Small sleep to prevent tight loop
                    }
                    backpressureActive.set(false)
                }
                BackpressureStrategy.FORCE_FLUSH -> {
                    // Force an immediate flush if we're not already flushing
                    if (flushing.compareAndSet(false, true)) {
                        logger.warn { "Backpressure applied: Forcing immediate flush due to capacity reached" }
                        runBlocking {
                            try {
                                val eventsToFlush = counter.getAndSet(0)
                                if (eventsToFlush > 0) {
                                    flushSafely()
                                }
                            } finally {
                                flushing.set(false)
                                backpressureActive.set(counter.get() >= maxCapacity)
                            }
                        }
                    }
                }
            }
        } else if (backpressureActive.get() && currentCount < (maxCapacity * 0.7).toInt()) {
            // Release backpressure when we're below 70% capacity
            backpressureActive.set(false)
            logger.info { "Backpressure released (current count: $currentCount)" }
        }
    }

    override fun onInsert() = increment()

    fun registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(
            Thread {
                logger.info("Shutdown hook triggered. Cleaning up...")
                stop()
                logger.info("Cleanup complete.")
            },
        )
    }

    // Method to check if backpressure is currently active
    fun isBackpressureActive(): Boolean = backpressureActive.get()

    // Method to get number of dropped events (relevant for DROP strategy)
    fun getDroppedEventCount(): Int = droppedEvents.get()

    // Method to get current event count
    fun getCurrentEventCount(): Int = counter.get()

    private fun scheduleIntervalFlush() {
        flushJob?.cancel() // Cancel any previous timer
        flushJob =
            scope.launch {
                delay(interval.withJitter((500.milliseconds)))
                logger.info { "Sjekker etter $interval om det skal flushes. Har ${counter.get()} events." }
                if (counter.get() == 0) {
                    // No need to flush if counter is zero
                    scheduleIntervalFlush()
                    return@launch
                }
                flushSafely()
                counter.set(0)
            }
    }

    private suspend fun flushSafely() {
        try {
            action.invoke()
        } catch (e: Exception) {
            logger.error(e) { "Feilet å flushe data: ${e.message}" }
            throw e
        } finally {
            scheduleIntervalFlush() // Reschedule after successful flush
        }
    }

    private companion object {
        private val logger = KotlinLogging.logger {}
    }

    private fun Duration.withJitter(tolerance: Duration) =
        this + (-tolerance.inWholeMilliseconds..tolerance.inWholeMilliseconds).random().milliseconds
}

class BackpressureException(message: String) : RuntimeException(message)
