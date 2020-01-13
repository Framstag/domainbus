package com.framstag.domainbus.jdbc

import java.sql.Connection

interface ConnectionFactory {
    fun connect(): Connection
}