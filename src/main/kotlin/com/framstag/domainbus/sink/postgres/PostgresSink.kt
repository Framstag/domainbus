package com.framstag.domainbus.sink.postgres

import com.framstag.domainbus.sink.Sink
import mu.KLogging
import java.time.Duration
import java.util.concurrent.TimeUnit

class PostgresSink(private val context: Context) : Sink, Runnable {
    companion object : KLogging()

    private val stateData: StateData = StateData()
    private val stateMap =
        mapOf(
            State.CONNECTING to mapOf(
                Outcome.SUCCESS to State.PROCESSING,
                Outcome.ERROR to State.CONNECTING
            ),
            State.PROCESSING to mapOf(
                Outcome.SUCCESS to State.PROCESSING,
                Outcome.ERROR to State.CLOSING
            ),
            State.CLOSING to mapOf(
                Outcome.SUCCESS to State.CONNECTING,
                Outcome.ERROR to State.CONNECTING
            )
        )

    private fun wait(duration: Duration) {
        if (duration.isZero) {
            return
        }
        val timeoutInNanos = duration.toMillis()
        logger.info("Sleeping for $timeoutInNanos milliseconds...")
        TimeUnit.MILLISECONDS.sleep(timeoutInNanos)
        logger.info("Sleeping for $timeoutInNanos milliseconds done.")
    }

    private fun getStateProcessor(state: State): StateProcessor {
        return when (state) {
            State.CONNECTING -> ConnectingState()
            State.PROCESSING -> ProcessingState()
            State.CLOSING -> ClosingState()
        }
    }

    private fun getNewState(currentState: State, outcome: Outcome): State {
        val entry = stateMap[currentState] ?: throw Exception("Internal configuration error")

        return entry[outcome] ?: throw Exception("Internal configuration error")
    }

    override fun run() {
        var lastState : State? = null
        var currentState = State.CONNECTING

        while (!Thread.currentThread().isInterrupted) {
            if (currentState != lastState) {
                logger.info("State $currentState()")
            }

            val processor = getStateProcessor(currentState)
            val result = processor.process(stateData, context)

            wait(result.timeout)

            lastState = currentState

            currentState = getNewState(currentState, result.outcome)

            if (result.outcome != Outcome.SUCCESS) {
                logger.info("$lastState() -> ${result.outcome}")
            }
        }

        close()
    }

    override fun close() {
        val currentState = State.CLOSING

        logger.info("$currentState()")

        val processor = getStateProcessor(currentState)
        val result = processor.process(stateData, context)

        logger.info("$currentState -> ${result.outcome}")
    }
}