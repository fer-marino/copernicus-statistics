package com.serco.sentinel1.wamp

import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.script.Script
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram
import org.elasticsearch.search.aggregations.metrics.avg.Avg
import org.elasticsearch.search.aggregations.metrics.max.Max
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController

@RestController
class StatisticsService {
    @Autowired
    private lateinit var esTemplate: ElasticsearchTemplate

    @RequestMapping(value = "/test", method = arrayOf(RequestMethod.GET))
    fun hourAggregate() {
        val dateHistogram = AggregationBuilders.dateHistogram("production_per_hour")
                .field("publishedHub").dateHistogramInterval(DateHistogramInterval.HOUR)
        dateHistogram.subAggregation( AggregationBuilders.avg("delay_avg")
                .script( Script("doc['publishedHub'].value - doc['start'].value")) )
        dateHistogram.subAggregation( AggregationBuilders.max("delay_max")
                .script( Script("doc['publishedHub'].value - doc['start'].value")) )

        // TODO sum(size)
        // TODO prod type?

        val query = NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.matchAllQuery())
                .addAggregation(dateHistogram)
                .build()
        esTemplate.query(query, {
            (it.aggregations.first() as Histogram).buckets.forEach {
                val avg = it.aggregations.get<Avg>("delay_avg")
                val max = it.aggregations.get<Max>("delay_max")
                println("Bucket ${it.key} count ${it.docCount} - ${avg.name} ${(avg.value/1000/60/60).toInt()} hour - ${max.name} - ${(max.value/1000/60/60).toInt()} hour")
            }
            })
    }
}