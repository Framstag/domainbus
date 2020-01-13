package com.framstag.domainbus

import mu.KotlinLogging
import java.sql.Connection

object Transaction {
    val logger = KotlinLogging.logger(Transaction::javaClass.name)

    fun execute(connection: Connection, transactedCode: (connection: Connection)-> Unit) {
        try {
            transactedCode(connection)

            connection.commit()
        }
        catch (e: Exception) {
            try {
                logger.error("SQL error, rolling back...",e)
                connection.rollback()
            }
            catch (e : Exception) {
                logger.error("SQL error during rolling back (ignored)",e)
            }

            throw e
        }
    }

}