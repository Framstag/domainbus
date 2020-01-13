package com.framstag.domainbus.event

/**
 * The result of processing an event
 */
data class EventResult(val serialId : Long, val success: Boolean)