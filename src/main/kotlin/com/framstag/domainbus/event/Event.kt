package com.framstag.domainbus.event

data class Event(val serialId: Long, val id : String, val hash : Int, val data : String)