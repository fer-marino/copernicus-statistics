package com.serco.sentinel1.wamp.model

import java.util.*


//@Document(indexName = "statshourly", type = "statsHourly", shards = 3, replicas = 0, refreshInterval = "-1")
data class StatsDataHourly(//@Id @Field(type = FieldType.keyword, fielddata = true)
                           var id: String = "",
                           var timestamp: Date = Date(),
                           var prodType: String = "",
                           var count: Long = 0,
                           var delay_avg: Long = 0,
                           var delay_max: Long = 0)


//@Document(indexName = "statsorbit", type = "statsOrbit", shards = 3, replicas = 0, refreshInterval = "-1")
data class StatsDataOrbit(//@Id @Field(type = FieldType.keyword, fielddata = true)
                          var id: String = "",
                          var orbit: Long, var prodType: String = "",
                          var count: Long = 0,
                          var delay_avg: Long = 0,
                          var delay_max: Long = 0)

//@Document(indexName = "statsdaily", type = "statsdaily", shards = 3, replicas = 0, refreshInterval = "-1")
data class StatsDataDaily(//@Id @Field(type = FieldType.keyword, fielddata = true)
                          var id: String = "",
                          var day: Date = Date(),
                          var prodType: String = "",
                          var count: Long = 0,
                          var delay_avg: Long = 0,
                          var delay_max: Long = 0)

//@RepositoryRestResource(collectionResourceRel = "hourlyStats", path = "hourlyStats")
//interface StatsHourlyRepository : PagingAndSortingRepository<StatsDataHourly, String>, ElasticsearchRepository<StatsDataHourly, String>
//
//@RepositoryRestResource(collectionResourceRel = "orbitStats", path = "orbitStats")
//interface StatsOrbitRepository: PagingAndSortingRepository<StatsDataOrbit, String>, ElasticsearchRepository<StatsDataOrbit, String>
//
//@RepositoryRestResource(collectionResourceRel = "dailyStats", path = "dailyStats")
//interface StatsDailyRepository : PagingAndSortingRepository<StatsDataDaily, String>, ElasticsearchRepository<StatsDataDaily, String>
