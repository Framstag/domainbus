package com.framstag.domainbus.sink

import com.framstag.domainbus.event.Event
import com.framstag.domainbus.event.EventBatch
import com.framstag.domainbus.event.EventResult
import com.framstag.domainbus.source.Source
import mu.KLogging
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

class ExecutorSink(private val source: Source, private val executor: ExecutorService, private val timeout: Duration) :
    Sink, Runnable {
    companion object : KLogging()

    private fun wait(duration: Duration) {
        if (duration.isZero) {
            return
        }
        val timeoutInNanos = duration.toMillis()
        logger.info("Sleeping for $timeoutInNanos milliseconds...")
        TimeUnit.MILLISECONDS.sleep(timeoutInNanos)
        logger.info("Sleeping for $timeoutInNanos milliseconds done.")
    }

    private fun processEvent(event: Event): EventResult {
        logger.info("Processing event ${event.serialId}...")

        TimeUnit.MILLISECONDS.sleep(timeout.toMillis())

        val result = EventResult(event.serialId, true)

        logger.info("Processing event ${event.serialId} done")

        return result
    }

    private fun process(eventProducer: CompletableFuture<Event>, eventResult: CompletableFuture<EventResult>) {
        eventProducer.thenAcceptAsync(Consumer<Event> { event ->
            eventResult.complete(processEvent(event))
        }, executor)
    }

    private fun process(): Duration {
        val batchProducer = CompletableFuture<EventBatch>()

        batchProducer.thenAcceptAsync { batch ->
            logger.debug("Consuming events...")
            for (event in batch.events) {
                process(event.event, event.result)
            }
            logger.debug("Consuming events done.")
        }

        logger.debug("Requesting data...")
        source.requestData(batchProducer)
        logger.debug("Requesting data done.")

        val batch = batchProducer.get()

        return batch.timeout
    }

    override fun run() {
        while (!Thread.currentThread().isInterrupted) {
            val timeout = process()

            wait(timeout)
        }

        close()
    }

    override fun close() {
        // TODO: Close Source
    }
}
