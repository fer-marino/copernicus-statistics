package com.serco.sentinel1.wamp.model

import org.springframework.data.annotation.Id
import org.springframework.data.annotation.LastModifiedDate
import org.springframework.data.elasticsearch.annotations.Document
import org.springframework.data.elasticsearch.annotations.Field
import org.springframework.data.elasticsearch.annotations.FieldType
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository
import org.springframework.data.repository.PagingAndSortingRepository
import org.springframework.data.rest.core.annotation.RepositoryRestResource
import org.springframework.web.bind.annotation.CrossOrigin
import java.util.*

@Document(indexName = "product", type = "product", shards = 3, replicas = 0, refreshInterval = "-1")
data class Product (@Id @Field(type = FieldType.keyword, fielddata = true) var name: String,
                    var start: Date,
                    var stop: Date,
                    @Field(type = FieldType.keyword) var mission: String,
                    @Field(type = FieldType.keyword) var dtId: String,
                    var orbit: Long,
                    @Field(type = FieldType.keyword) var prodType: String,
                    @Field(type = FieldType.keyword) var timeliness: String?,
                    @Field(type = FieldType.keyword) var crc: String,
                    @LastModifiedDate var publishedHub: Date,
                    var attributes: Map<String?, String?>) {
    constructor(): this("", Date(), Date(), "", "", 0, "",
            null, "", Date(), mapOf())
}

@CrossOrigin(origins = arrayOf("http://localhost:3000"))
@RepositoryRestResource(collectionResourceRel = "product", path = "product")
interface ProductRepository: PagingAndSortingRepository<Product, String>, ElasticsearchRepository<Product, String>