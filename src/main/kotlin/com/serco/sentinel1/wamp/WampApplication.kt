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
        val camelConfig = DefaultCamelContext()
        camelConfig.addRoutes(dhusPolling)
        camelConfig.start()
    }


}

fun main(args: Array<String>) {
    SpringApplication.run(WampApplication::class.java, *args)
}
