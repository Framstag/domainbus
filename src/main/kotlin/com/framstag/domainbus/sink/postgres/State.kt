package com.framstag.domainbus.sink.postgres

enum class State {
    CONNECTING,
    PROCESSING,
    CLOSING
}