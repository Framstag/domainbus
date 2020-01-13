package com.framstag.domainbus.sink

import com.framstag.domainbus.event.Event
import com.framstag.domainbus.event.EventBatch
import com.framstag.domainbus.event.EventResult
import com.framstag.domainbus.source.Source
import mu.KLogging
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class SimpleSink(private val source: Source) : Sink, Runnable {
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

    private fun process(eventProducer: CompletableFuture<Event>, eventResult: CompletableFuture<EventResult>) {
        eventProducer.thenAccept() { event ->
            logger.info("ProcessingState ${event.serialId}")

            eventResult.complete(EventResult(event.serialId, true))
        }
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
