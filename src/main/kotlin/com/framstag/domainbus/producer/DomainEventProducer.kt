package com.framstag.domainbus.producer

import com.framstag.domainbus.jdbc.ConnectionFactory
import com.framstag.domainbus.Transaction
import mu.KLogging
import java.sql.Connection
import java.sql.Statement
import java.util.*
import kotlin.math.absoluteValue
import kotlin.random.Random

class DomainEventProducer(private val connectionFactory: ConnectionFactory) : Runnable {
    companion object : KLogging() {
        const val INSERT_DOMAIN_EVENT = "INSERT INTO DOMAIN_OUT (id,hash,data) VALUES (?,?,?)"
    }

    private fun insertEvents(connection: Connection) {
        val eventCount = Random.nextInt(1, 3)
        logger.info("Inserting $eventCount events...")

        Transaction.execute(connection) {
            val statement = connection.prepareStatement(
                INSERT_DOMAIN_EVENT,
                Statement.RETURN_GENERATED_KEYS
            )

            statement.use {
                for (x in 1..eventCount) {
                    val id = UUID.randomUUID().toString()

                    statement.setString(1, id)
                    statement.setInt(2, id.hashCode().rem(20).absoluteValue)
                    statement.setString(3, "{}")

                    statement.executeUpdate()

                    val result = statement.generatedKeys

                    if (result != null && result.next()) {
                        val latestSerialId = result.getLong(1)
                        logger.info("serialId: $latestSerialId")
                    }
                }
            }
        }
    }

    override fun run() {
        val connection = connectionFactory.connect()

        connection.use {
            try {
                while (!Thread.currentThread().isInterrupted) {
                    Thread.sleep(Random.nextLong(100, 200))

                    insertEvents(connection)
                }
            } catch (e: InterruptedException) {
                logger.info("Producer interrupted, stopping...")
            } catch (e: Exception) {
                logger.info("Producer errored, stopping...",e)
            }
        }

        logger.info("Producer stopped")
    }
}