package com.framstag.domainbus.event

import java.util.concurrent.CompletableFuture

data class TransitEvent(val event : CompletableFuture<Event>, var result : CompletableFuture<EventResult>)