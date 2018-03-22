package com.serco.sentinel1.wamp.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "wamp")
open class WampConfig {
    var dhusId: String = ""
    var dhusUser: String = ""
    var dhusPassword: String = ""
    var dhusUrl: String = ""
    var dhusIngestedFrom: String = ""
    var connectionTimeout: Int = 30000
    var readTimeout: Int = 30000
    var extra: String = ""
    var upsert: Boolean = true
    var filter: String = ""
    var pollInterval: String = "5m"
    var autoStart: Boolean = true
    var reindex: Boolean = false
    var odaInbox: String = ""
}