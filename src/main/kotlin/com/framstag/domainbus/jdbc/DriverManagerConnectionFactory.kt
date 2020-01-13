package com.framstag.domainbus.jdbc

import java.sql.Connection
import java.sql.DriverManager
import java.util.*

class DriverManagerConnectionFactory(private val config : ConnectionConfig) : ConnectionFactory {
    override fun connect():Connection {
        val props = Properties()

        props.setProperty("user",config.user)
        props.setProperty("password",config.password)
        props.setProperty("loginTimeout", config.loginTimeout.seconds.toString())

        val connection = DriverManager.getConnection(config.url, props)

        connection.autoCommit = false

        return connection
    }
}