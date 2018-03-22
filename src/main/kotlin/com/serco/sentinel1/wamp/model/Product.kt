package com.serco.sentinel1.wamp.model

import com.esri.core.geometry.Geometry
import com.esri.core.geometry.OperatorExportToGeoJson
import com.fasterxml.jackson.annotation.JsonRawValue
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.PagingAndSortingRepository
import java.io.IOException
import java.time.LocalDateTime
import javax.persistence.Column
import javax.persistence.ElementCollection
import javax.persistence.Entity
import javax.persistence.Id

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
        @JsonRawValue @Column(columnDefinition = "text") var footprint: String? = null,
        var publishedHub: LocalDateTime? = null,
        var publishedOda: LocalDateTime? = null,
        var pac: String? = null,
        var odaSubscription: String? = null,
        @ElementCollection @Column(columnDefinition = "text")
        var attributes: MutableMap<String?, String?> = mutableMapOf()
)

interface ProductRepository: CrudRepository<Product, String> {
    @Query("SELECT max(p.publishedHub) from Product p")
    fun getMaxPublishedHub(): LocalDateTime?
    @Query("SELECT p FROM Product p WHERE p.publishedHub BETWEEN ?1 AND ?2 ORDER BY p.publishedHub")
    fun findAllIn(start: LocalDateTime, stop: LocalDateTime): Iterable<Product>
}

class GeometryToStringSerializer : JsonSerializer<Geometry>() {

    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(input: Geometry, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider) {
        jsonGenerator.writeObject(OperatorExportToGeoJson.local().execute(input))
    }

}