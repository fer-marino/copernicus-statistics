package com.serco.sentinel1.wamp

import com.esri.core.geometry.*
import com.fasterxml.jackson.databind.ObjectMapper
import com.serco.sentinel1.wamp.config.WampConfig
import com.serco.sentinel1.wamp.model.ProductNew
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.ClearScrollRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchScrollRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilders.matchAllQuery
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.Scroll
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.text.SimpleDateFormat
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalUnit
import java.util.*
import java.util.concurrent.TimeUnit


@RestController
class ProductService {
    @Autowired lateinit var esClient: RestHighLevelClient

    @RequestMapping("/api/rebuildProduct")
    fun rebuildIndex(@RequestParam(value = "from", required = true) from: String,
                     @RequestParam(value = "to", required = true) to: String,
                     @RequestParam(value = "pageSize", defaultValue = "100") pageSize: Int,
                     @RequestParam(value = "update", defaultValue = "false") update: Boolean): ResponseEntity<String> {

        if(!update) {
            try {
                esClient.indices().deleteIndex(DeleteIndexRequest(to))
            } catch (e: ElasticsearchException) {
                println(" *** Delete index $to failed: ${e.message}")
            }

            // create index with mapping
            val jsonString = """
            {
                "settings" : {
                    "number_of_shards" : 3,
                    "number_of_replicas" : 1
                },
                "mappings": {
                    "$to": {
                        "properties": {
                            "footprint": {
                                "type": "geo_shape"
                            },
                            "crc": {
                                "type": "keyword"
                            },
                            "dtId": {
                              "type": "keyword"
                            },
                            "mission": {
                              "type": "keyword"
                            },
                            "name": {
                              "type": "text"
                            },
                            "orbit": {
                              "type": "long"
                            },
                            "prodType": {
                              "type": "keyword"
                            },
                            "publishedHub": {
                              "type": "date"
                            },
                            "start": {
                              "type": "date"
                            },
                            "stop": {
                              "type": "date"
                            },
                            "timeliness": {
                              "type": "keyword"
                            }
                        }
                    }
                }
            }
            """
            esClient.lowLevelClient.performRequest("PUT", to, mapOf(), NStringEntity(jsonString, ContentType.APPLICATION_JSON))
        }

        Thread {
            val scroll =  Scroll(TimeValue.timeValueHours(1))
            val searchSourceBuilder = SearchSourceBuilder()
                    .query(matchAllQuery())
                    .size(pageSize)
                    .timeout(TimeValue.timeValueSeconds(10))
            val searchRequest = SearchRequest(from).source(searchSourceBuilder).scroll(scroll)

            var searchResponse = esClient.search(searchRequest)
            var scrollId = searchResponse.scrollId
            var hits = searchResponse.hits.hits

            var scrollRequest: SearchScrollRequest

            val om = ObjectMapper()
            val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
            val mapper = ObjectMapper()

            val totalHits = searchResponse.hits.totalHits
            var count = 0;
            println(" *** Rebuild started")
            var start: Long
            val overallStart = System.currentTimeMillis()
            while (hits != null && hits.isNotEmpty()) {
                start = System.currentTimeMillis()
                val bulkRequest = BulkRequest().timeout(TimeValue.timeValueSeconds(30))
                hits.forEach {
                    try {
                        val res = om.readValue(it.sourceAsString, Map::class.java)
                        val attributes = res["attributes"] as Map<String?, String?>

                        var geometry = OperatorImportFromWkt.local().execute(WktImportFlags.wktImportDefaults,
                                Geometry.Type.Polygon,
                                attributes["JTS footprint"]!!,
                                null)
                        geometry = OperatorSimplifyOGC.local().execute(geometry, SpatialReference.create(4326), true, null)
                        val geoJson = OperatorExportToGeoJson.local().execute(geometry)

                        val prod = ProductNew(res["name"] as String,
                                sdf.format(Date(res["start"] as Long)),
                                sdf.format(Date(res["stop"] as Long)),
                                res["mission"] as String,
                                res["dtId"] as String,
                                res["orbit"] as Int,
                                res["prodType"] as String,
                                res["timeliness"] as? String,
                                res["crc"] as String,
                                geoJson,
                                sdf.format(Date(res["publishedHub"] as Long)),
                                attributes)

                        synchronized(bulkRequest, {
                            if(update)
                                bulkRequest.add(UpdateRequest(to, to, prod.name).upsert(mapper.writeValueAsString(prod), XContentType.JSON))
                            else
                                bulkRequest.add(IndexRequest(to, to, prod.name).source(mapper.writeValueAsString(prod), XContentType.JSON))

                        })

                        count++
                    } catch (e: Exception) {
                        println("Error during product retrieval of ${it.id}. Skipping. Detailed message: ${e.message}")
                    } catch (e: NullPointerException) {
                        println("NPE during product retrieval of ${it.id}. Line ${e.stackTrace[0]}, ${e.stackTrace[1]}")
                    }
                }

                scrollRequest = SearchScrollRequest(scrollId).scroll(scroll)
                searchResponse = esClient.searchScroll(scrollRequest)

                scrollId = searchResponse.scrollId
                hits = searchResponse.hits.hits

                try {
                    val it = esClient.bulk(bulkRequest)
                    it.forEach {
                        if (it.opType == DocWriteResponse.Result.UPDATED)
                            println("Product ${it.id} updated")
                    }
                } catch (e: ElasticsearchException) {
                    error("Something horrible happened during bulk ingestion: ${e.message}")
                }

                // stats
                val prodPerSec = pageSize.toDouble() / ((System.currentTimeMillis() - start).toDouble()/1000)
                val estimateDuration =  Duration.of(((totalHits - count) / prodPerSec).toLong(), ChronoUnit.SECONDS)
                val elapsedTime = Duration.of(System.currentTimeMillis() - overallStart, ChronoUnit.MILLIS)
                print("\r *** Product migrated $count/$totalHits (${Math.round(count.toDouble()/totalHits *10000)/100.toDouble()}%)" +
                        " as ${Math.round(prodPerSec*1000).toDouble()/1000} prod/sec. Elapsed ${elapsedTime.pretty()}, Remaining ${estimateDuration.pretty()}")
            }

            println(" *** Migration completed in ${Duration.of(System.currentTimeMillis() - overallStart, ChronoUnit.MILLIS).pretty()}")
        }.start()

        return ResponseEntity.ok("Import started")
    }

}

fun Duration.pretty(): String {
    val seconds = seconds
    val absSeconds = Math.abs(seconds);
    val positive = String.format(
        "%d:%02d:%02d",
        absSeconds / 3600,
        (absSeconds % 3600) / 60,
        absSeconds % 60)
    return if(seconds < 0) "-" + positive else positive
}
