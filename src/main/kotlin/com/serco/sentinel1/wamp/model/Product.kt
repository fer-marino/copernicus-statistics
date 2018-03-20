package com.serco.sentinel1.wamp.model

import com.esri.core.geometry.Geometry
import com.esri.core.geometry.OperatorExportToGeoJson
import com.fasterxml.jackson.annotation.JsonRawValue
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import org.springframework.data.annotation.LastModifiedDate
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.PagingAndSortingRepository
import java.io.IOException
import java.time.LocalDateTime
import java.util.*
import javax.persistence.*

@Entity
data class Product (
        @Id var name: String = "",
        var start: LocalDateTime = LocalDateTime.now(),
        var stop: LocalDateTime = LocalDateTime.now(),
        var mission: String = "",
        var dtId: String = "",
        var orbit: Int = 0,
        var prodType: String = "",
        var timeliness: String? = null,
        var crc: String = "",
        @JsonRawValue var footprint: String? = null,
        var publishedHub: LocalDateTime? = null,
        var publishedOda: LocalDateTime? = null,
        var pac: String? = null,
        var odaSubscription: String? = null,
        @ElementCollection var attributes: Map<String?, String?> = mapOf()
)

interface ProductRepository: PagingAndSortingRepository<Product, String> {
    @Query("SELECT max(p.publishedHub) from Product p")
    fun getMaxPublishedHub(): LocalDateTime?
//    @Query("SELECT p.* from Product p WHERE p.publishedHub BETWEEN ? AND ? ORDER BY p.publishedHub")
//    fun findAllIn(start: LocalDateTime, stop: LocalDateTime): Iterable<Product>
}

class GeometryToStringSerializer : JsonSerializer<Geometry>() {

    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(input: Geometry, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider) {
        jsonGenerator.writeObject(OperatorExportToGeoJson.local().execute(input))
    }

}