package com.framstag.domainbus

import com.framstag.domainbus.jdbc.ConnectionConfig
import com.framstag.domainbus.jdbc.ConnectionFactory
import com.framstag.domainbus.jdbc.DriverManagerConnectionFactory
import com.framstag.domainbus.producer.DomainEventProducerPool
import com.framstag.domainbus.sink.postgres.PostgresSink
import com.framstag.domainbus.source.postgres.PostgresSource
import mu.KotlinLogging
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

val logger = KotlinLogging.logger("main")

fun main() {
    val connectionConfig =
        ConnectionConfig("jdbc:pgsql://localhost/postgres", "postgres", "postgres")
    val connectionFactory = DriverManagerConnectionFactory(connectionConfig)

    logger.info("Creating Schema...")

    val schemaGenerator = SchemaGenerator()

    schemaGenerator.generate(connectionFactory)

    val executor = Executors.newFixedThreadPool(10)

    val source = PostgresSource(com.framstag.domainbus.source.postgres.Context(connectionFactory))
    //val source = InMemorySource()

    val sink = PostgresSink(com.framstag.domainbus.sink.postgres.Context(connectionFactory,source))
    //val sink = SimpleSink(source)
    //val sink = DelayedSimpleSink(source, Duration.ofSeconds(2))
    //val sink = ExecutorSink(source, executor,Duration.ofSeconds(2))

    logger.info("Starting producer pool...")
    val producerPool = DomainEventProducerPool(connectionFactory)

    producerPool.start()
    logger.info("Starting producer pool... done")

    val sinkThread = Thread(sink)

    logger.info("Starting sink thread...")
    sinkThread.start()
    logger.info("Starting sink thread done")

    TimeUnit.SECONDS.sleep(10)

    logger.info("Stopping producers...")
    producerPool.stop()
    logger.info("Stopping producers done")

    logger.info("Stopping sink...")
    sinkThread.interrupt()
    sinkThread.join()
    logger.info("Stopping sink done")

    logger.info("Stopping executors...")
    executor.shutdown()
    executor.awaitTermination(30, TimeUnit.SECONDS)
    logger.info("done.")
}

