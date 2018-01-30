package com.serco.sentinel1.wamp

import com.serco.sentinel1.wamp.config.ElasticsearchConfig
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner

@RunWith(SpringJUnit4ClassRunner::class)
@ContextConfiguration(classes = arrayOf(WampApplication::class, ElasticsearchConfig::class))
class TestRepository {
    @Autowired lateinit var productService: ProductService

    @Test
    fun test1() {
        productService.rebuildIndex("product", "products", 100, false)
        Thread.sleep(10000000)
    }
}