package com.serco.sentinel1.wamp.model

import com.esri.core.geometry.Geometry
import com.esri.core.geometry.OperatorExportToGeoJson
import com.fasterxml.jackson.annotation.JsonRawValue
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import org.springframework.data.annotation.LastModifiedDate
import java.io.IOException
import java.util.*


data class Product (
                    var name: String = "",
                    var start: Date = Date(),
                    var stop: Date = Date(),
                    var mission: String = "",
                    var dtId: String = "",
                    var orbit: Long = 0,
                    var prodType: String = "",
                    var timeliness: String? = null,
                    var crc: String = "",
                    @LastModifiedDate var publishedHub: Date = Date(),
                    var attributes: Map<String?, String?> = mapOf())

data class ProductNew (
        var name: String = "",
        var start: String = "",
        var stop: String = "",
        var mission: String = "",
        var dtId: String = "",
        var orbit: Int = 0,
        var prodType: String = "",
        var timeliness: String? = null,
        var crc: String = "",
        @JsonRawValue var footprint: String?,
        @LastModifiedDate var publishedHub: String = "",
        var attributes: Map<String?, String?> = mapOf()) {

}


class GeometryToStringSerializer : JsonSerializer<Geometry>() {

    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(input: Geometry, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider) {
        jsonGenerator.writeObject(OperatorExportToGeoJson.local().execute(input))
    }

}