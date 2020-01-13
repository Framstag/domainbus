package com.framstag.domainbus.source.postgres

import com.framstag.domainbus.event.EventBatch
import com.framstag.domainbus.event.EventResult
import com.framstag.domainbus.jdbc.ConnectionFactory
import io.mockk.*
import mu.KLogging
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.*
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class RequestDataTest {

    companion object : KLogging()

    private lateinit var connectionFactory: ConnectionFactory
    private lateinit var context: Context
    private lateinit var source: PostgresSource
    private lateinit var connection: Connection
    private lateinit var statement: PreparedStatement
    private lateinit var resultSet: ResultSet

    @BeforeEach
    fun beforeEach() {
        connection = mockk()
        connectionFactory = mockk()
        statement = mockk()
        resultSet = mockk()

        every {
            connectionFactory.connect()
        } returns connection

        every {
            connection.close()
        } just runs

        every {
            connection.prepareStatement(any())
        } returns statement

        every {
            statement.queryTimeout = any()
        } just runs

        every {
            statement.close()
        } just runs

        context = Context(
            connectionFactory,
            sessionTimeout = Duration.ofSeconds(1)
        )

        source = PostgresSource(context)

        val producer = CompletableFuture<EventBatch>()

        source.requestData(producer)

        producer.get()
    }

    @Test
    fun retrieveDataSuccessTest() {
        // Select events
        every {
            statement.executeQuery()
        } returns resultSet

        // Result set
        every {
            resultSet.next()
        } returns true andThen false

        every {
            resultSet.getLong(1)
        } returns 1
        every {
            resultSet.getString(2)
        } returns "1"
        every {
            resultSet.getInt(3)
        } returns 1
        every {
            resultSet.getString(4)
        } returns "{}"

        // Update
        every {
            statement.setLong(1, any())
        } just runs

        every {
            statement.executeUpdate()
        } returns 1

        every {
            connection.commit()
        } just runs

        val producer = CompletableFuture<EventBatch>()

        producer.thenAcceptAsync { batch ->
            for (transitEvent in batch.events) {
                transitEvent.event.thenAccept { event ->
                    transitEvent.result.complete(EventResult(event.serialId, true))
                }
            }
        }

        source.requestData(producer)

        val eventBatch = producer.get()

        assertEquals(1, eventBatch.events.size)
        assertEquals(Duration.ZERO, eventBatch.timeout)
        assertEquals(State.PROCESSING, source.currentState)

        source.close()

        assertEquals(State.CONNECTING, source.currentState, "Expect state CONNECTING")

        verifySequence {
            connectionFactory.connect()
            connection.prepareStatement(any())
            statement.queryTimeout = 1
            statement.executeQuery()
            resultSet.next()
            resultSet.getLong(1)
            resultSet.getString(2)
            resultSet.getInt(3)
            resultSet.getString(4)
            resultSet.next()
            statement.close()
            connection.prepareStatement(any())
            statement.queryTimeout = 1
            statement.setLong(1, 1)
            statement.executeUpdate()
            statement.close()
            connection.commit()
            connection.close()
        }
    }

    @Test
    fun retrieveDataErrorTest() {
        // Select events
        every {
            statement.executeQuery()
        } throws SQLException("Error selecting data")

        every {
            connection.rollback()
        } just runs

        val producer = CompletableFuture<EventBatch>()

        source.requestData(producer)

        val eventBatch = producer.get()

        assertEquals(0, eventBatch.events.size)
        assertEquals(context.selectQueryTimeout, eventBatch.timeout)
        assertEquals(State.CLOSING, source.currentState)

        source.close()

        assertEquals(State.CONNECTING, source.currentState, "Expect state CONNECTING")

        verifySequence {
            connectionFactory.connect()
            connection.prepareStatement(any())
            statement.queryTimeout = any()
            statement.executeQuery()
            statement.close()
            connection.rollback()
            connection.close()
        }
    }

    @Test
    fun retrieveDataTimeoutTest() {
        // Select events
        every {
            statement.executeQuery()
        } answers {
            logger.info("Waiting...")
            TimeUnit.SECONDS.sleep(context.selectQueryTimeout.toSeconds())
            logger.info("Waiting...done")
            logger.info("Creating exception")
            throw SQLException("Select Timeout")
        }

        every {
            connection.rollback()
        } just runs

        val producer = CompletableFuture<EventBatch>()

        source.requestData(producer)

        val eventBatch = producer.get()

        assertEquals(0, eventBatch.events.size)
        assertEquals(context.selectQueryTimeout, eventBatch.timeout)
        assertEquals(State.CLOSING, source.currentState)

        source.close()

        assertEquals(State.CONNECTING, source.currentState, "Expect state CONNECTING")

        verifySequence {
            connectionFactory.connect()
            connection.prepareStatement(any())
            statement.queryTimeout = any()
            statement.executeQuery()
            statement.close()
            connection.rollback()
            connection.close()
        }
    }

    @Test
    fun ackTimeoutTest() {
        // Select events
        every {
            statement.executeQuery()
        } returns resultSet

        // Result set
        every {
            resultSet.next()
        } returns true andThen false

        every {
            resultSet.getLong(1)
        } returns 1
        every {
            resultSet.getString(2)
        } returns "1"
        every {
            resultSet.getInt(3)
        } returns 1
        every {
            resultSet.getString(4)
        } returns "{}"

        // Update
        every {
            statement.setLong(1, any())
        } just runs

        every {
            statement.executeUpdate()
        } returns 1

        every {
            connection.commit()
        } just runs

        val producer = CompletableFuture<EventBatch>()

        producer.thenAcceptAsync { batch ->
            for (transitEvent in batch.events) {
                transitEvent.event.thenAccept { event ->
                    val timeout = context.sessionTimeout.plusSeconds(1).toSeconds()
                    logger.info("Waiting $timeout seconds (1 second beyond session timeout) for acking event...")
                    TimeUnit.SECONDS.sleep(timeout)
                    logger.info("Acking event...")
                    transitEvent.result.complete(EventResult(event.serialId, true))
                    logger.info("Acking event...done")
                }
            }
        }

        source.requestData(producer)

        val eventBatch = producer.get()

        logger.info("Waiting for end of session timeout...")

        TimeUnit.MILLISECONDS.sleep(context.sessionTimeout.toMillis())

        logger.info("Waiting for end of session timeout...done")

        assertEquals(1, eventBatch.events.size)
        assertTrue(eventBatch.events[0].result.isCancelled)
        assertEquals(Duration.ZERO, eventBatch.timeout)
        assertEquals(State.PROCESSING, source.currentState)

        source.close()

        assertEquals(State.CONNECTING, source.currentState, "Expect state CONNECTING")

        verifySequence {
            connectionFactory.connect()
            connection.prepareStatement(any())
            statement.queryTimeout = 1
            statement.executeQuery()
            resultSet.next()
            resultSet.getLong(1)
            resultSet.getString(2)
            resultSet.getInt(3)
            resultSet.getString(4)
            resultSet.next()
            statement.close()
            connection.commit()
            connection.close()
        }
    }
}