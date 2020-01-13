package com.framstag.domainbus.source.postgres

import com.framstag.domainbus.event.EventBatch
import java.util.concurrent.CompletableFuture

internal interface StateProcessor {
    fun process(stateData : StateData, producer: CompletableFuture<EventBatch>, context : Context):ProcessResult
}