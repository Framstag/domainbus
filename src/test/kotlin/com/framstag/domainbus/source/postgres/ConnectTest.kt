package com.framstag.domainbus.source.postgres

import com.framstag.domainbus.event.EventBatch
import com.framstag.domainbus.jdbc.ConnectionFactory
import io.mockk.*
import mu.KLogging
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.sql.Connection
import java.sql.SQLException
import java.time.Duration
import java.util.concurrent.*

class ConnectTest {

    companion object : KLogging()

    @Test
    fun connectSuccessTest() {
        val connection = mockk<Connection>()
        val connectionFactory = mockk<ConnectionFactory>()

        every {
            connectionFactory.connect()
        } returns connection

        every {
            connection.close()
        } just runs

        val context = Context(connectionFactory)
        val source = PostgresSource(context)

        val producer1 = CompletableFuture<EventBatch>()

        source.requestData(producer1)

        val eventBatch1 = producer1.get()

        assertEquals(0, eventBatch1.events.size, "Expect empty batch")
        assertEquals(Duration.ZERO, eventBatch1.timeout, "Expect zero timeout")
        assertEquals(State.PROCESSING, source.currentState, "Expect state PROCESSING")

        source.close()

        assertEquals(State.CONNECTING, source.currentState, "Expect state CONNECTING")

        verifySequence {
            connectionFactory.connect()
            connection.close()
        }
    }

    @Test
    fun connectErrorTest() {
        val connectionFactory = mockk<ConnectionFactory>()

        every {
            connectionFactory.connect()
        } throws SQLException("Connection error")

        val context = Context(connectionFactory)
        val source = PostgresSource(context)

        val producer1 = CompletableFuture<EventBatch>()

        source.requestData(producer1)

        val eventBatch1 = producer1.get()

        assertEquals(0, eventBatch1.events.size, "Expect empty batch")
        assertEquals(context.deleteTimeout, eventBatch1.timeout, "Expect delete timeout")
        assertEquals(State.CONNECTING, source.currentState, "Expect state CONNECTING")

        source.close()

        assertEquals(State.CONNECTING, source.currentState, "Expect state CONNECTING")

        verifySequence {
            connectionFactory.connect()
        }

    }

    @Test
    fun connectTimeoutTest() {
        val connection = mockk<Connection>()
        val connectionFactory = mockk<ConnectionFactory>()

        every {
            connectionFactory.connect()
        } answers {
            logger.info("Waiting...")
            TimeUnit.SECONDS.sleep(2)
            logger.info("Waiting...done")
            logger.info("Returning connection")
            connection
        }

        every {
            connection.close()
        } just runs

        val context = Context(connectionFactory)
        val source = PostgresSource(context)

        val producer1 = CompletableFuture<EventBatch>()

        source.requestData(producer1)

        assertThrows<TimeoutException> {
            logger.info("Waiting for batch...")
            producer1.get(100, TimeUnit.MILLISECONDS)
        }

        logger.info("Waiting for batch...done")

        producer1.get()

        source.close()

        assertEquals(State.CONNECTING, source.currentState, "Expect state CONNECTING")

        verifySequence {
            connectionFactory.connect()
            connection.close()
        }
    }

    @Test
    fun connectSuccessAndCloseSuccessTest() {
        val connection = mockk<Connection>()
        val connectionFactory = mockk<ConnectionFactory>()

        every {
            connectionFactory.connect()
        } returns connection

        every {
            connection.close()
        } just runs

        val context = Context(connectionFactory)
        val source = PostgresSource(context)

        val producer1 = CompletableFuture<EventBatch>()

        source.requestData(producer1)

        val eventBatch1 = producer1.get()

        assertEquals(0, eventBatch1.events.size, "Expect empty batch")
        assertEquals(Duration.ZERO, eventBatch1.timeout, "Expect zero timeout")
        assertEquals(State.PROCESSING, source.currentState, "Expect state PROCESSING")

        source.close()

        assertEquals(State.CONNECTING, source.currentState, "Expect state CONNECTING")

        verifySequence {
            connectionFactory.connect()
            connection.close()
        }
    }

    @Test
    fun connectSuccessAndCloseErrorTest() {
        val connection = mockk<Connection>()
        val connectionFactory = mockk<ConnectionFactory>()

        every {
            connectionFactory.connect()
        } returns connection

        every {
            connection.close()
        } throws SQLException("Cannot close")

        val context = Context(connectionFactory)
        val source = PostgresSource(context)

        val producer1 = CompletableFuture<EventBatch>()

        source.requestData(producer1)

        val eventBatch1 = producer1.get()

        assertEquals(0, eventBatch1.events.size, "Expect empty batch")
        assertEquals(Duration.ZERO, eventBatch1.timeout, "Expect zero timeout")
        assertEquals(State.PROCESSING, source.currentState, "Expect state PROCESSING")

        source.close()

        assertEquals(State.CONNECTING, source.currentState, "Expect state CONNECTING")

        verifySequence {
            connectionFactory.connect()
            connection.close()
        }
    }
}