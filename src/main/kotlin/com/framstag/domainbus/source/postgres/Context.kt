package com.framstag.domainbus.source.postgres

import com.framstag.domainbus.jdbc.ConnectionFactory
import java.time.Duration

/**
 * Immutable state part of postgres source
 */
data class Context(val connectionFactory : ConnectionFactory,
                   val connectionErrorTimeout : Duration = Duration.ofSeconds(1),
                   val selectQueryTimeout : Duration = Duration.ofSeconds(1),
                   val deleteTimeout : Duration = Duration.ofSeconds(1),
                   val sessionTimeout : Duration = Duration.ofSeconds(30),
                   val emptySelectTimeout : Duration = Duration.ofMillis(100))