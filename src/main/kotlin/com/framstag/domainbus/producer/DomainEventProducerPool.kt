package com.framstag.domainbus.producer

import com.framstag.domainbus.jdbc.ConnectionFactory
import mu.KLogging
import java.util.*

class DomainEventProducerPool(private val connectionFactory: ConnectionFactory) {
    companion object : KLogging()

    private val thread = Thread {loop()}
    private val threadPool = Vector<Thread>()
    private var shutdown = false

    private fun startThreads() {
        for (t in 1..10) {
            threadPool.add(Thread(DomainEventProducer(connectionFactory)))
        }

        for (thread in threadPool) {
            thread.start()
        }
    }

    private fun stopThreads() {
        for (thread in threadPool) {
            logger.info("Stopping thread ${thread.name}...")
            thread.interrupt()
            logger.info("Waiting for thread to shutdown...")
            thread.join()
            logger.info("Waiting for thread to shutdown...done")
        }

        threadPool.clear()
    }

    private fun loop() {
        startThreads()

        try {
            while (!shutdown) {
                Thread.sleep(1000)
            }
        } catch (e: InterruptedException) {
            logger.info("DomainEventProducerPool interrupted, stopping...")
        }

        logger.info("ProducerPool shutting down...")

        stopThreads()

        logger.info("ProducerPool shutting down..done")
    }

    fun start() {
        thread.start()
    }

    fun stop() {
        shutdown = true
        thread.interrupt()
        thread.join()
    }
}