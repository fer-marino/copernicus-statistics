package com.serco.sentinel1.wamp

import com.serco.sentinel1.wamp.config.WampConfig
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.RestController

@RestController
class StatisticsService {
    @Autowired lateinit var config: WampConfig


}

