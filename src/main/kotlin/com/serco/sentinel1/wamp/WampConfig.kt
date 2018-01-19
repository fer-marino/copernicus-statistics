package com.serco.sentinel1.wamp

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "wamp")
data class WampConfig (var dhusId: String, var dhusUser: String, var dhusPassword: String, var dhusUrl: String)