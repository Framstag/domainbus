package com.framstag.domainbus.source.postgres

import com.framstag.domainbus.event.EventBatch
import com.framstag.domainbus.source.Source
import mu.KLogging
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

class PostgresSource(private val context : Context) : Source {
    companion object : KLogging()

    private val executor = Executors.newSingleThreadExecutor()
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

    private var lastState : State? = null
    var currentState = State.CONNECTING
        private set

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

    private fun process(producer: CompletableFuture<EventBatch>) {
        if (currentState != lastState) {
            logger.info("State $currentState()")
        }

        val processor = getStateProcessor(currentState)
        val result = processor.process(stateData, producer, context)

        lastState = currentState

        currentState = getNewState(currentState, result.outcome)

        if (result.outcome != Outcome.SUCCESS) {
            logger.info("$lastState() -> ${result.outcome}")
        }
    }

    override fun requestData(producer: CompletableFuture<EventBatch>) {
        // Make sure that actual processing is asynchronous but serialized
        CompletableFuture.runAsync(
            Runnable {
                process(producer)
            },
            executor)
    }

    override fun close() {
        logger.info("Closing source...")

        val producer = CompletableFuture<EventBatch>()

        // Queue closing of connection
        val result = CompletableFuture.runAsync(
            Runnable {
                currentState = State.CLOSING
                process(producer)
            },
            executor)

        // Wait for result
        result.get()

        // Wait for processing
        producer.get()

        // Wait for the executor service to shutdown
        executor.shutdown()
        logger.info("Closing source...done")
    }
}