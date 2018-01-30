package com.serco.sentinel1.wamp

import com.serco.sentinel1.wamp.config.WampConfig
import com.serco.sentinel1.wamp.model.Product
import org.apache.camel.impl.DefaultCamelContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import org.springframework.data.rest.core.config.RepositoryRestConfiguration
import org.springframework.data.rest.webmvc.config.RepositoryRestConfigurerAdapter
import javax.annotation.PostConstruct


@SpringBootApplication
@EnableConfigurationProperties(WampConfig::class)
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

@Configuration // FIXME no more needed?
class ExposeAllRepositoryRestConfiguration : RepositoryRestConfigurerAdapter() {

    override fun configureRepositoryRestConfiguration(config: RepositoryRestConfiguration?) {
        super.configureRepositoryRestConfiguration(config)
        config?.exposeIdsFor(Product::class.java)
    }

}


fun main(args: Array<String>) {
    SpringApplication.run(WampApplication::class.java, *args)
}
