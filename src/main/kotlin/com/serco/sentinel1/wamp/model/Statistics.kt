package com.serco.sentinel1.wamp.model

import java.util.*

data class StatsData (var timestamp: Date, var prodType: String, var count: Long, var delay_avg: Long, var delay_max: Long)