package com.framstag.domainbus.source

import com.framstag.domainbus.event.*
import mu.KLogging
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import kotlin.math.absoluteValue

class InMemorySource : Source {
    companion object : KLogging()

    private var serialId = 1L

    override fun requestData(producer: CompletableFuture<EventBatch>) {
        logger.info("BEGIN SOURCE Transaction")

        val events = mutableListOf<TransitEvent>()

        for (i in 1..50) {
            val id = UUID.randomUUID().toString()
            val hash = id.hashCode().rem(20).absoluteValue

            logger.info("Reading event $serialId")

            val event = Event(serialId, id, hash, id)

            serialId++

            val eventProducer = CompletableFuture.completedFuture(event)
            val eventResultConsumer = CompletableFuture<EventResult>()
            val transitEvent = TransitEvent(eventProducer, eventResultConsumer)

            events.add(transitEvent)
        }

        val batch = EventBatch(events, Duration.ZERO)

        logger.info("Completing producer...")
        producer.completeAsync {
            logger.info("Completing producer done")
            batch
        }

        logger.info("Waiting for event results...")
        for (event in batch.events) {
            event.result.get()
        }
        logger.info("Waiting for event results done")

        logger.info("END SOURCE Transaction")
    }

    override fun close() {
    }

}