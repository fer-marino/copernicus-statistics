package com.serco.sentinel1.wamp.model

import org.springframework.data.annotation.Id
import org.springframework.data.elasticsearch.annotations.Document
import org.springframework.data.elasticsearch.annotations.Field
import org.springframework.data.elasticsearch.annotations.FieldType
import org.springframework.data.repository.PagingAndSortingRepository
import org.springframework.data.rest.core.annotation.RepositoryRestResource
import java.util.*

@Document(indexName = "product", type = "product", shards = 3, replicas = 0, refreshInterval = "-1")
data class Product (@Id var id: UUID,
                    @Field(type = FieldType.keyword) var name: String,
                    var start: Date,
                    var stop: Date,
                    @Field(type = FieldType.keyword) var mission: String,
                    @Field(type = FieldType.keyword) var dtId: String,
                    var orbit: Long,
                    @Field(type = FieldType.keyword) var prodType: String,
                    @Field(type = FieldType.keyword) var timeliness: String?,
                    @Field(type = FieldType.keyword) var crc: String,
                    var publishedHub: Date,
                    var attributes: Map<String?, String?>)

@RepositoryRestResource(collectionResourceRel = "product", path = "product")
interface ProductRepository: PagingAndSortingRepository<Product, UUID>