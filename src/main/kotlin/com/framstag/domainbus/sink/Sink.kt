package com.framstag.domainbus.sink

import com.framstag.domainbus.event.Event
import com.framstag.domainbus.event.EventResult
import java.util.concurrent.CompletableFuture

interface Sink {
    fun close()
}