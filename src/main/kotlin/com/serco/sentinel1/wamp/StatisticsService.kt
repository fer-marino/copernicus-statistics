package com.serco.sentinel1.wamp

import com.serco.sentinel1.wamp.model.StatsData
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.script.Script
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.metrics.avg.Avg
import org.elasticsearch.search.aggregations.metrics.max.Max
import org.joda.time.DateTime
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
import java.util.*

@RestController
class StatisticsService {
    @Autowired
    private lateinit var esTemplate: ElasticsearchTemplate

    @RequestMapping(value = "/statistics/hourly", method = arrayOf(RequestMethod.GET))
    fun hourAggregate(): List<StatsData> {
        val dateHistogram = AggregationBuilders.dateHistogram("production_per_hour")
                .field("publishedHub").dateHistogramInterval(DateHistogramInterval.HOUR)
        val prodTypeAgg = AggregationBuilders.terms("prod_type").field("prodType")
        dateHistogram.subAggregation( prodTypeAgg )

        prodTypeAgg.subAggregation( AggregationBuilders.avg("delay_avg")
                .script( Script("doc['publishedHub'].value - doc['start'].value")) )
        prodTypeAgg.subAggregation( AggregationBuilders.max("delay_max")
                .script( Script("doc['publishedHub'].value - doc['start'].value")) )

        val query = NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.matchAllQuery())
                .addAggregation(dateHistogram)
                .build()

        val output= mutableListOf<StatsData>()
        esTemplate.query(query, {
            it.aggregations.get<Histogram>("production_per_hour").buckets.forEach {
                val date = (it.key as DateTime).toDate()
                it.aggregations.get<Terms>("prod_type").buckets.forEach {
                    val avg = it.aggregations.get<Avg>("delay_avg")
                    val max = it.aggregations.get<Max>("delay_max")

                    output.add(StatsData(date, it.keyAsString, it.docCount, avg.value.toLong(), max.value.toLong()))
                }
            }
        })

        return output
    }
}

