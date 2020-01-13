package com.framstag.domainbus

import com.framstag.domainbus.jdbc.ConnectionFactory
import java.sql.SQLException

class SchemaGenerator {

    companion object {
        const val DROP_TABLE_DOMAIN_OUT =
            """
                DROP TABLE DOMAIN_OUT
            """

        const val CREATE_TABLE_DOMAIN_OUT =
            """
                CREATE TABLE DOMAIN_OUT(
                  serialId SERIAL PRIMARY KEY,
                  id VARCHAR(36) NOT NULL,
                  hash INT NOT NULL,
                  data VARCHAR(1000) NOT NULL
                )
            """

        const val DROP_TABLE_DOMAIN_SINK =
            """
                DROP TABLE DOMAIN_SINK
            """
        const val CREATE_TABLE_DOMAIN_SINK =
            """
                CREATE TABLE DOMAIN_SINK(
                  serialId SERIAL PRIMARY KEY,
                  id VARCHAR(36) NOT NULL,
                  hash INT NOT NULL,
                  data VARCHAR(1000) NOT NULL
                )
            """

        const val DROP_TABLE_TOPIC =
            """
                DROP TABLE TOPIC
            """

        const val CREATE_TABLE_TOPIC =
            """
                CREATE TABLE TOPIC(
                  serialId SERIAL PRIMARY KEY,
                  source VARCHAR(20) NOT NULL,
                  id VARCHAR(36) NOT NULL,
                  data VARCHAR(1000) NOT NULL
                )
            """
    }

    fun generate(connectionFactory: ConnectionFactory) {
        val connection = connectionFactory.connect()

        connection.use {
            val statement = connection.createStatement()


            Transaction.execute(connection) {
                try {
                    logger.info("Delete existing tables...")
                    statement.execute(DROP_TABLE_DOMAIN_OUT)
                    statement.execute(DROP_TABLE_DOMAIN_SINK)
                    statement.execute(DROP_TABLE_TOPIC)
                    logger.info("Delete existing tables...done")
                } catch (e: SQLException) {
                    logger.warn("Ignoring exception during deletion of old schema", e)
                }
            }

            Transaction.execute(connection) {
                logger.info("Creating tables...")
                statement.execute(CREATE_TABLE_DOMAIN_OUT)
                statement.execute(CREATE_TABLE_DOMAIN_SINK)
                statement.execute(CREATE_TABLE_TOPIC)
                logger.info("Creating tables...done")
            }
        }
    }
}