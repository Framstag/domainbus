package com.framstag.domainbus.sink.postgres

import com.framstag.domainbus.Transaction
import com.framstag.domainbus.event.Event
import com.framstag.domainbus.event.EventBatch
import com.framstag.domainbus.event.EventResult
import mu.KLogging
import java.sql.SQLIntegrityConstraintViolationException
import java.util.concurrent.CompletableFuture

internal class ProcessingState : StateProcessor {
    companion object : KLogging() {
        const val INSERT_EVENT = "INSERT INTO DOMAIN_SINK (serialId,id,hash,data) VALUES (?,?,?,?)"
    }

    private fun consume(stateData : StateData,eventProducer: CompletableFuture<Event>, eventResult: CompletableFuture<EventResult>) {
        eventProducer.thenApply { event->
            logger.info("Processing ${event.serialId}")

            stateData.connection?.let { connection ->
                Transaction.execute(connection) {
                    val statement = connection.prepareStatement(INSERT_EVENT)

                    statement.use {
                        statement.queryTimeout = 1 // in seconds
                        statement.setLong(1, event.serialId)
                        statement.setString(2,event.id)
                        statement.setInt(3,event.hash)
                        statement.setString(4,event.data)

                        try {
                            val result = statement.executeUpdate()

                            if (result == 0) {
                                logger.warn("Event ${event.serialId} was not inserted in db")
                            }

                            eventResult.complete(EventResult(event.serialId, true))
                        }
                        catch (e : SQLIntegrityConstraintViolationException) {
                            logger.warn("Event ${event.serialId} was already inserted in db, acknowledging anyway")
                            eventResult.complete(EventResult(event.serialId, true))
                        }
                    }
                }
            }
        }
    }

    override fun process(stateData: StateData, context: Context): ProcessResult {
        val batchProducer = CompletableFuture<EventBatch>()

        batchProducer.thenAcceptAsync {batch ->
            logger.debug("Consuming events...")
            for (event in batch.events) {
                consume(stateData,event.event,event.result)
            }
            logger.debug("Consuming events done.")
        }

        logger.debug("Requesting data...")
        context.source.requestData(batchProducer)
        logger.debug("Requesting data done.")

        val batch = batchProducer.get()

        return ProcessResult(Outcome.SUCCESS, batch.timeout)
    }
}