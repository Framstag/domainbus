package com.framstag.domainbus.source.postgres

import com.framstag.domainbus.event.EventBatch
import com.framstag.domainbus.event.TransitEvent
import mu.KLogging
import java.sql.SQLException
import java.time.Duration
import java.util.concurrent.CompletableFuture

internal class ClosingState : StateProcessor {
    companion object : KLogging()

    override fun process(stateData : StateData, producer: CompletableFuture<EventBatch>, context: Context): ProcessResult {
        stateData.connection?.let { connection ->
            try {
                connection.close()
            }
            catch (e: SQLException) {
                logger.error("Error while closing connection (ignored)",e)
            }

            stateData.connection = null
        }

        val batch = EventBatch(listOf<TransitEvent>(),Duration.ZERO)

        producer.complete(batch)

        return ProcessResult(Outcome.SUCCESS)
    }
}