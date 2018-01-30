package com.serco.sentinel1.wamp

import com.fasterxml.jackson.databind.ObjectMapper
import com.serco.sentinel1.wamp.model.StatsDataDaily
import com.serco.sentinel1.wamp.model.StatsDataHourly
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.script.Script
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.metrics.avg.Avg
import org.elasticsearch.search.aggregations.metrics.max.Max
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.joda.time.DateTime
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.text.SimpleDateFormat
import java.util.*

@RestController
class StatisticsService {
    @Autowired lateinit var esClient: RestHighLevelClient

    private val aggregationHourly: DateHistogramAggregationBuilder get() {
        val dateHistogram = AggregationBuilders.dateHistogram("production_per_hour")
                .field("publishedHub").dateHistogramInterval(DateHistogramInterval.HOUR)
        val prodTypeAgg = AggregationBuilders.terms("prod_type").field("prodType")

        dateHistogram.subAggregation( prodTypeAgg )

        prodTypeAgg.subAggregation( AggregationBuilders.avg("delay_avg")
                .script( Script("doc['publishedHub'].value - doc['start'].value")) )
        prodTypeAgg.subAggregation( AggregationBuilders.max("delay_max")
                .script( Script("doc['publishedHub'].value - doc['start'].value")) )
        prodTypeAgg.subAggregation( AggregationBuilders.count("count") )

        return dateHistogram
    }

    @RequestMapping(value = "/api/statistics/deltaHour", method = arrayOf(RequestMethod.GET))
    fun buildHorulyDelta(@RequestParam(value = "delta", required = true) deltaString: String): ResponseEntity<List<StatsDataHourly>> {
        val delta = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(deltaString)

        val filter = QueryBuilders.rangeQuery("publishedHub").gte(delta.time)

        val query = SearchSourceBuilder().query(filter).aggregation(aggregationHourly)
        val output= mutableListOf<StatsDataHourly>()

        esClient.search(SearchRequest("product").source(query)).aggregations.get<Histogram>("production_per_hour").buckets.forEach {
            val date = (it.key as DateTime).toDate()
            it.aggregations.get<Terms>("prod_type").buckets.forEach {
                val avg = it.aggregations.get<Avg>("delay_avg")
                val max = it.aggregations.get<Max>("delay_max")

                output.add(StatsDataHourly(date.toString() + it.keyAsString, date, it.keyAsString, it.docCount, avg.value.toLong(), max.value.toLong()))
            }
        }

        output.forEach { esClient.indexAsync(buildIndexRequestHourly(it), ActionListener.wrap({}, { error("Something horrible appened during ingestion ${it.message}") })) }

        return ResponseEntity.ok(output)
    }

    private fun buildIndexRequestHourly(stat: StatsDataHourly): IndexRequest {
        val indexRequest = IndexRequest("statshourly", "statshourly", stat.id)
        val objectMapper = ObjectMapper()
        return indexRequest.source( objectMapper.writeValueAsString(stat), XContentType.JSON )
    }

    @RequestMapping(value = "/api/statistics/rebuildHourly", method = arrayOf(RequestMethod.GET))
    fun buildHoruly(): ResponseEntity<String> {
        val req = DeleteRequest("statshourly", "statshourly", "*")
        esClient.delete(req)

        val query = SearchSourceBuilder().aggregation(aggregationHourly)

        esClient.searchAsync(SearchRequest("product").source(query), ActionListener.wrap({
                it.aggregations.get<Histogram>("production_per_hour").buckets.forEach {
                    val date = (it.key as DateTime).toDate()
                    it.aggregations.get<Terms>("prod_type").buckets.forEach {
                        val avg = it.aggregations.get<Avg>("delay_avg")
                        val max = it.aggregations.get<Max>("delay_max")

                        val add = StatsDataHourly(date.toString()+it.keyAsString, date, it.keyAsString, it.docCount, avg.value.toLong(), max.value.toLong())

                        esClient.indexAsync(buildIndexRequestHourly(add), ActionListener.wrap({}, { error("Something horrible appened during ingestion ${it.message}") }))
                    }
                }
            }, { error("Error during ingestion ${it.message}") })
        )

        return ResponseEntity.ok("Rebuild started")
    }

    @RequestMapping(value = "/api/statistics/rebuildDaily", method = arrayOf(RequestMethod.GET))
    fun buildDaily(): ResponseEntity<String> {
        val req = DeleteRequest("statshourly", "statshourly", "*")
        esClient.delete(req)

        val dateHistogram = AggregationBuilders.dateHistogram("production_per_day")
                .field("publishedHub").dateHistogramInterval(DateHistogramInterval.DAY)
        val prodTypeAgg = AggregationBuilders.terms("prod_type").field("prodType")

        dateHistogram.subAggregation( prodTypeAgg )

        prodTypeAgg.subAggregation( AggregationBuilders.avg("delay_avg")
                .script( Script("doc['publishedHub'].value - doc['start'].value")) )
        prodTypeAgg.subAggregation( AggregationBuilders.max("delay_max")
                .script( Script("doc['publishedHub'].value - doc['start'].value")) )

        val output= mutableListOf<StatsDataDaily>()
//        esTemplate.query(NativeSearchQueryBuilder().withIndices("product")
//                .withQuery(QueryBuilders.matchAllQuery()).addAggregation(dateHistogram).build(), {
//            it.aggregations.get<Histogram>("production_per_day").buckets.forEach {
//                val date = (it.key as DateTime).toDate()
//                it.aggregations.get<Terms>("prod_type").buckets.forEach {
//                    val avg = it.aggregations.get<Avg>("delay_avg")
//                    val max = it.aggregations.get<Max>("delay_max")
//
//                    output.add(StatsDataDaily(date.toString()+it.keyAsString, date, it.keyAsString, it.docCount, avg.value.toLong(), max.value.toLong()))
//                }
//            }
//        })

//        output.forEach { dailyRepository.save(it) }
        return ResponseEntity.ok("Processing Started")
    }

    @RequestMapping("/api/statistics/lastIngestedProduct")
    fun lastIngestionDate(): ResponseEntity<Date> {
        val searchQuery = SearchRequest("product").source( SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery())
                .aggregation( AggregationBuilders.max("max_publication").field("publishedHub") )
        )

        val value = esClient.search(searchQuery).aggregations.get<Max>("max_publication").value
        val out = Date( if(value.isFinite()) value.toLong() else 0 )

        return ResponseEntity.ok(out);
    }
}

