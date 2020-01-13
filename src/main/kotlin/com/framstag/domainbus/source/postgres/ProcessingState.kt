package com.framstag.domainbus.source.postgres

import com.framstag.domainbus.event.Event
import com.framstag.domainbus.Transaction
import com.framstag.domainbus.event.EventBatch
import com.framstag.domainbus.event.EventResult
import com.framstag.domainbus.event.TransitEvent
import mu.KLogging
import java.sql.Connection
import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

internal class ProcessingState : StateProcessor {
    companion object : KLogging() {
        const val SELECT_EVENTS =
            """
                SELECT 
                  serialId,
                  id,
                  hash,
                  data
                FROM DOMAIN_OUT
                ORDER BY 
                  serialId
                FOR UPDATE SKIP LOCKED
                LIMIT 50
            """

        const val DELETE_EVENT =
            """
                DELETE 
                FROM DOMAIN_OUT
                WHERE
                  serialId = ?
            """
    }

    private fun selectEvents(connection: Connection, context: Context): List<TransitEvent> {
        val eventList: MutableList<TransitEvent> = mutableListOf()

        val statement = connection.prepareStatement(SELECT_EVENTS)

        statement.use {
            statement.queryTimeout = context.selectQueryTimeout.seconds.toInt()

            val resultSet = statement.executeQuery()

            while (resultSet.next()) {
                val event = Event(
                    resultSet.getLong(1),
                    resultSet.getString(2),
                    resultSet.getInt(3),
                    resultSet.getString(4)
                )

                val resultConsumer = CompletableFuture<EventResult>()

                val transitEvent =
                    TransitEvent(CompletableFuture.completedFuture(event), resultConsumer)

                eventList.add(transitEvent)
            }
        }

        return eventList
    }

    private fun acknowledgeEvent(connection: Connection, serialId: Long, context: Context) {
        logger.info("Deleting event $serialId...")

        val statement = connection.prepareStatement(DELETE_EVENT)

        statement.use {
            statement.queryTimeout = context.deleteTimeout.seconds.toInt()
            statement.setLong(1, serialId)

            val result = statement.executeUpdate()

            if (result != 1) {
                logger.error("Event with id $serialId could not be acknowledged, since it is not available anymore")
            }
        }
    }

    private fun waitForEventsUntilSessionTimeout(
        connection: Connection,
        iterator: ListIterator<TransitEvent>,
        sessionBegin: Instant,
        context: Context
    ) {
        while (iterator.hasNext()) {
            val currentTime = Instant.now()
            val delta = ChronoUnit.MILLIS.between(sessionBegin, currentTime)

            if (delta < context.sessionTimeout.toMillis()) {
                val timeout = context.sessionTimeout.minus(delta, ChronoUnit.MILLIS)
                val transitEvent = iterator.next()
                val event = transitEvent.event.get()
                logger.info("Waiting ${timeout.toMillis()} milliseconds for event ${event.serialId} to get processed...")

                try {
                    val result = transitEvent.result.get(timeout.toMillis(), TimeUnit.MILLISECONDS)

                    if (result.success) {
                        // TODO: Check for error and report
                        acknowledgeEvent(connection, result.serialId, context)
                    } else {
                        logger.info("Event ${result.serialId} was not successfully processed, not acknowledged")
                    }
                } catch (e: TimeoutException) {
                    transitEvent.result.cancel(false)
                    logger.warn("Event ${event.serialId} canceled since it was not processing within session timeout")
                }
            } else {
                break
            }
        }
    }

    private fun collectAcksAfterSessionTimeout(
        connection: Connection,
        iterator: ListIterator<TransitEvent>,
        context: Context
    ) {
        while (iterator.hasNext()) {
            val transitEvent = iterator.next()
            val event = transitEvent.event.get()

            // If already committed, cancel is ignored, thus we blindly cancel and afterwards, if it is done
            transitEvent.result.cancel(false)

            if (transitEvent.result.isDone) {
                val result = transitEvent.result.get()

                if (result.success) {
                    // TODO: Check for error and report
                    acknowledgeEvent(connection, result.serialId, context)
                } else {
                    logger.warn("Event ${event.serialId} was not successfully processed, not acknowledged")
                }
            } else {
                logger.warn("Event ${event.serialId} canceled since it was not processing within session timeout")
            }
        }
    }

    private fun handleAcks(
        connection: Connection,
        events: List<TransitEvent>,
        sessionBegin: Instant,
        context: Context
    ) {
        val iterator = events.listIterator()

        // Consume with timeout until session timeout is reached
        waitForEventsUntilSessionTimeout(connection, iterator, sessionBegin, context)

        // If session timeout is reached, just check, if
        collectAcksAfterSessionTimeout(connection, iterator, context)
    }

    override fun process(
        stateData: StateData,
        producer: CompletableFuture<EventBatch>,
        context: Context
    ): ProcessResult {
        val sessionBegin = Instant.now()

        var timeout = context.emptySelectTimeout

        try {
            stateData.connection?.let { connection ->
                Transaction.execute(connection) {
                    val events = selectEvents(connection, context)

                    if (events.isNotEmpty()) {
                        timeout = Duration.ZERO
                        logger.info("Selected ${events.size} entries")
                    }

                    val batch = EventBatch(events, timeout)
                    producer.complete(batch)

                    handleAcks(connection, events, sessionBegin, context)
                }
            }
        } catch (e: SQLException) {
            if (!producer.isDone) {
                val batch = EventBatch(listOf(), context.selectQueryTimeout)
                producer.complete(batch)
            }

            return ProcessResult(Outcome.ERROR)
        }

        return ProcessResult(Outcome.SUCCESS)
    }
}