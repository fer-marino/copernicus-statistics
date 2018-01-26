package com.serco.sentinel1.wamp

import com.serco.sentinel1.wamp.model.StatsDailyRepository
import com.serco.sentinel1.wamp.model.StatsDataDaily
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.script.Script
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.metrics.avg.Avg
import org.elasticsearch.search.aggregations.metrics.max.Max
import org.joda.time.DateTime
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.text.SimpleDateFormat
import java.util.*

@RestController
class StatisticsService {
    @Autowired private lateinit var esTemplate: ElasticsearchTemplate

    @Autowired lateinit var dailyRepository: StatsDailyRepository

    private val aggregation: DateHistogramAggregationBuilder get() {
        val dateHistogram = AggregationBuilders.dateHistogram("production_per_hour")
                .field("publishedHub").dateHistogramInterval(DateHistogramInterval.HOUR)
        val prodTypeAgg = AggregationBuilders.terms("prod_type").field("prodType")

        dateHistogram.subAggregation( prodTypeAgg )

        prodTypeAgg.subAggregation( AggregationBuilders.avg("delay_avg")
                .script( Script("doc['publishedHub'].value - doc['start'].value")) )
        prodTypeAgg.subAggregation( AggregationBuilders.max("delay_max")
                .script( Script("doc['publishedHub'].value - doc['start'].value")) )

        return dateHistogram
    }

    @RequestMapping(value = "/api/statistics/deltaHour", method = arrayOf(RequestMethod.GET))
    fun buildHorulyDelta(@RequestParam(value = "delta", required = true) deltaString: String): ResponseEntity<List<StatsDataDaily>> {
        val delta = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(deltaString)

        val filter = QueryBuilders.rangeQuery("publishedHub").gte(delta.time)

        val query = NativeSearchQueryBuilder().withIndices("product").withQuery(filter).addAggregation(aggregation).build()
        val output= mutableListOf<StatsDataDaily>()

        esTemplate.query(query, {
            it.aggregations.get<Histogram>("production_per_hour").buckets.forEach {
                val date = (it.key as DateTime).toDate()
                it.aggregations.get<Terms>("prod_type").buckets.forEach {
                    val avg = it.aggregations.get<Avg>("delay_avg")
                    val max = it.aggregations.get<Max>("delay_max")

                    output.add(StatsDataDaily(date.toString()+it.keyAsString, date, it.keyAsString, it.docCount, avg.value.toLong(), max.value.toLong()))
                }
            }
        })

//        output.forEach { dailyRepository.save(it) }

        return ResponseEntity.ok(output)
    }

    @RequestMapping(value = "/api/statistics/rebuildHourly", method = arrayOf(RequestMethod.GET))
    fun buildHoruly(): ResponseEntity<List<StatsDataDaily>> {
        dailyRepository.deleteAll()

        val output= mutableListOf<StatsDataDaily>()
        esTemplate.query(NativeSearchQueryBuilder().withIndices("product")
                .withQuery(QueryBuilders.matchAllQuery()).addAggregation(aggregation).build(), {
            it.aggregations.get<Histogram>("production_per_hour").buckets.forEach {
                val date = (it.key as DateTime).toDate()
                it.aggregations.get<Terms>("prod_type").buckets.forEach {
                    val avg = it.aggregations.get<Avg>("delay_avg")
                    val max = it.aggregations.get<Max>("delay_max")

                    output.add(StatsDataDaily(date.toString()+it.keyAsString, date, it.keyAsString, it.docCount, avg.value.toLong(), max.value.toLong()))
                }
            }
        })

        output.forEach { dailyRepository.save(it) }
        return ResponseEntity.ok(output)
    }

    @RequestMapping("/api/statistics/lastIngestedProduct")
    fun lastIngestionDate(): ResponseEntity<Date> {
        val searchQuery = NativeSearchQueryBuilder().withIndices("product")
                .withQuery(QueryBuilders.matchAllQuery())
                .addAggregation( AggregationBuilders.max("max_publication").field("publishedHub") )
                .build()

        var out = Date()
        esTemplate.query(searchQuery, {
            val value = it.aggregations.get<Max>("max_publication").value
            out = Date( if(value.isFinite()) value.toLong() else 0 )

        })

        return ResponseEntity.ok(out);
    }
}

