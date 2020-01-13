package com.framstag.domainbus.source

import com.framstag.domainbus.event.EventBatch
import java.util.concurrent.CompletableFuture

interface Source {
    /**
     * Request a batch of data from the source
     */
    fun requestData(producer: CompletableFuture<EventBatch>)

    /**
     * Close the source and free all resources.
     *
     * If the source have been close you cannot restart the source but have to create a new one.
     */
    fun close()
}