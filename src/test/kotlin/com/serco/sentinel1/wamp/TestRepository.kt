package com.serco.sentinel1.wamp

import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner

@RunWith(SpringJUnit4ClassRunner::class)
@ContextConfiguration(classes = arrayOf(WampApplication::class))
class TestRepository {
    @Autowired lateinit var statisticsService: StatisticsService

    @Test
    fun test1() {
        statisticsService.hourAggregate()
    }
}