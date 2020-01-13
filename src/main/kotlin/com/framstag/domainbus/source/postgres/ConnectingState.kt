package com.framstag.domainbus.source.postgres

import com.framstag.domainbus.event.EventBatch
import com.framstag.domainbus.event.TransitEvent
import mu.KLogging
import java.sql.SQLException
import java.time.Duration
import java.util.concurrent.CompletableFuture

internal class ConnectingState : StateProcessor {
    companion object : KLogging()

    private fun setEmptyBatch(producer: CompletableFuture<EventBatch>, duration : Duration) {
        // We offer an empty batch
        val batch = EventBatch(listOf<TransitEvent>(),duration)
        producer.complete(batch)
    }

    override fun process(stateData : StateData, producer: CompletableFuture<EventBatch>, context: Context): ProcessResult {
        return try {
            logger.info("Connecting...")
            stateData.connection = context.connectionFactory.connect()
            logger.info("Connecting done")

            setEmptyBatch(producer,Duration.ZERO)

            ProcessResult(Outcome.SUCCESS)
        } catch (e: SQLException) {
            logger.error("Error while connecting", e)

            setEmptyBatch(producer,context.connectionErrorTimeout)

            ProcessResult(Outcome.ERROR)
        }
    }
}