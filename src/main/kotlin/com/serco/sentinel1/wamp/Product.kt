package com.serco.sentinel1.wamp

import org.springframework.data.annotation.Id
import org.springframework.data.elasticsearch.annotations.Document
import org.springframework.data.repository.PagingAndSortingRepository
import org.springframework.data.rest.core.annotation.RepositoryRestResource
import java.util.*

@Document(indexName = "product", type = "product", shards = 1, replicas = 0, refreshInterval = "-1")
data class Product (@Id var id: UUID, var name: String,
                    var start: Date, var stop: Date, var mission: String,
                    var dtId: String, var orbit: Long, var prodType: String, var timeliness: String?,
                    var crc: String, var publishedHub: Date, var attributes: Map<String?, String?>)

@RepositoryRestResource(collectionResourceRel = "product", path = "product")
interface ProductRepository: PagingAndSortingRepository<Product, UUID>//, QuerydslPredicateExecutor<Product>