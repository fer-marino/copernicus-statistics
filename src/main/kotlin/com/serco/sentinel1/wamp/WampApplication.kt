package com.serco.sentinel1.wamp

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication


@SpringBootApplication
class WampApplication

fun main(args: Array<String>) {
    SpringApplication.run(WampApplication::class.java, *args)
}
