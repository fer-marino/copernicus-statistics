package com.serco.sentinel1.wamp

import org.springframework.data.cassandra.mapping.PrimaryKey
import org.springframework.data.cassandra.mapping.Table
import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.PagingAndSortingRepository
import org.springframework.data.rest.core.annotation.RepositoryRestResource
import java.time.LocalDateTime
import java.util.*

@Table
data class Product (@PrimaryKey var id: UUID, var name: String,
                    var start: LocalDateTime, var stop: LocalDateTime, var mission: String,
                    var dtId: String, var orbit: Long, var prodType: String, var timeliness: String?,
                    var crc: String, var publishedHub: LocalDateTime, var attributes: Map<String?, String?>)

@RepositoryRestResource(collectionResourceRel = "product", path = "product")
interface ProductRepository: PagingAndSortingRepository<Product, UUID>