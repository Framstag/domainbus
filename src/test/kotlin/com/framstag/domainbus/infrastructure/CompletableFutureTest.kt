package com.framstag.domainbus.infrastructure

import mu.KLogging
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.Executors.newSingleThreadExecutor

class CompletableFutureTest {
    companion object : KLogging()

    /**
     * No real tests, just makes sure by logger output that a SingleThreadExecutor queues
     * calls to runAsync() (using BlockingQueue) but serializes actual execution
     */
    @Test
    fun singleThreadPoolTest() {
        val executorService = newSingleThreadExecutor()

        for (n in 1..10) {
            logger.info("Start $n...")
            CompletableFuture.runAsync(Runnable {
                logger.info("Runnable $n")
            }, executorService)
            logger.info("Start $n... done")
        }

        executorService.shutdown()
    }
}