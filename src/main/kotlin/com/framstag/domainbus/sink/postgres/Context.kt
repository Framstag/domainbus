package com.framstag.domainbus.sink.postgres

import com.framstag.domainbus.jdbc.ConnectionFactory
import com.framstag.domainbus.source.Source

data class Context(val connectionFactory : ConnectionFactory, val source : Source)