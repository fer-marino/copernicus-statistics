package com.serco.sentinel1.wamp.model

import org.springframework.data.elasticsearch.annotations.Document
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository
import org.springframework.data.repository.PagingAndSortingRepository
import org.springframework.data.rest.core.annotation.RepositoryRestResource
import org.springframework.web.bind.annotation.CrossOrigin
import java.util.*


@Document(indexName = "statsoaily", type = "statsDaily", shards = 3, replicas = 0, refreshInterval = "-1")
data class StatsDataDaily(var id: String, var timestamp: Date, var prodType: String, var count: Long, var delay_avg: Long, var delay_max: Long)

@Document(indexName = "statsorbit", type = "statsOrbit", shards = 3, replicas = 0, refreshInterval = "-1")
data class StatsDataOrbit(var id: String, var orbit: Long, var prodType: String, var count: Long, var delay_avg: Long, var delay_max: Long)

@CrossOrigin(origins = arrayOf("http://localhost:3000"))
@RepositoryRestResource(collectionResourceRel = "dailyStats", path = "dailyStats")
interface StatsDailyRepository: PagingAndSortingRepository<StatsDataDaily, String>, ElasticsearchRepository<StatsDataDaily, String>

@CrossOrigin(origins = arrayOf("http://localhost:3000"))
@RepositoryRestResource(collectionResourceRel = "orbitStats", path = "orbitStats")
interface StatsOrbitRepository: PagingAndSortingRepository<StatsDataOrbit, String>, ElasticsearchRepository<StatsDataOrbit, String>
