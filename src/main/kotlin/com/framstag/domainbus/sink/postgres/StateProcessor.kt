package com.framstag.domainbus.sink.postgres

internal interface StateProcessor {
    fun process(stateData : StateData, context: Context):ProcessResult
}