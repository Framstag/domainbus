package com.framstag.domainbus.source.postgres

enum class State {
    CONNECTING,
    PROCESSING,
    CLOSING
}