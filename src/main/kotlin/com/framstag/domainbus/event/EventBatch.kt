package com.framstag.domainbus.event

import java.time.Duration

data class EventBatch(val events : List<TransitEvent>, val timeout : Duration)