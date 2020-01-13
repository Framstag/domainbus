package com.framstag.domainbus.sink.postgres

import mu.KLogging
import java.sql.SQLException
import java.time.Duration

internal class ClosingState : StateProcessor {
    companion object : KLogging()

    override fun process(stateData: StateData, context: Context): ProcessResult {
        stateData.connection?.let { connection ->
            try {
                connection.close()
            }
            catch (e: SQLException) {
                logger.error("Error while closing connection (ignored)",e)
            }

            stateData.connection = null
        }

        return ProcessResult(Outcome.SUCCESS,Duration.ZERO)
    }
}