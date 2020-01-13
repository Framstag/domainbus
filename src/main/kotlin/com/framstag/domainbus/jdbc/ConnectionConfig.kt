package com.framstag.domainbus.jdbc

import java.time.Duration

data class ConnectionConfig(
    val url: String,
    val user: String,
    val password: String,
    val loginTimeout: Duration = Duration.ofSeconds(2)
)