package com.serco.sentinel1.wamp

import org.apache.camel.impl.DefaultCamelContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import javax.annotation.PostConstruct


@SpringBootApplication
class WampApplication {
    @Autowired lateinit var dhusPolling: DhusPolling

    @PostConstruct
    fun postConstruct() {
        // work around waiting for camel 2.21 with official support to spring boot 2
        val camelConfig = DefaultCamelContext()
        camelConfig.addRoutes(dhusPolling)
        camelConfig.start()
    }


}

fun main(args: Array<String>) {
    SpringApplication.run(WampApplication::class.java, *args)
}
