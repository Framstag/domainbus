package com.framstag.domainbus.sink.postgres

import java.time.Duration

data class ProcessResult(val outcome : Outcome, val timeout : Duration)