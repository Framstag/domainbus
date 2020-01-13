package com.framstag.domainbus.source.postgres

import java.sql.Connection

/**
 * Mutable private state of postgres source
 */
internal class StateData(var connection: Connection? = null)