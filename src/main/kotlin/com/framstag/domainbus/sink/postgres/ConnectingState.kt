package com.framstag.domainbus.sink.postgres

import mu.KLogging
import java.sql.SQLException
import java.time.Duration

internal class ConnectingState : StateProcessor {
    companion object : KLogging()

    override fun process(stateData: StateData, context: Context): ProcessResult {
        return try {
            stateData.connection = context.connectionFactory.connect()

            ProcessResult(Outcome.SUCCESS, Duration.ZERO)
        } catch (e: SQLException) {
            logger.error("Error while connecting", e)

            ProcessResult(Outcome.ERROR, Duration.ofSeconds(1))

        }
    }
}