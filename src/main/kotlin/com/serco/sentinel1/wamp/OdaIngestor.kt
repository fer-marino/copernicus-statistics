package com.serco.sentinel1.wamp

import com.serco.sentinel1.wamp.config.WampConfig
import com.serco.sentinel1.wamp.model.Product
import com.serco.sentinel1.wamp.model.ProductRepository
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.dataformat.csv.CsvDataFormat
import org.apache.commons.csv.CSVFormat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

@Component
class OdaIngestor: RouteBuilder() {
    @Autowired lateinit var config: WampConfig
    @Autowired lateinit var productRepository: ProductRepository

    override fun configure() {
        from("file:${config.odaInbox}?include=.*.csv&delay=60000").routeId("Oda-Ingestor")
                .log("Processing \${in.headers.CamelFileName}")
                .unmarshal(CsvDataFormat(CSVFormat.EXCEL.withDelimiter(';').withFirstRecordAsHeader())).split(body())
                .process {
                    val dtfName = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")
                    val dtfOda = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    val body = it.`in`.body as List<String>
                    val prod = Product(pac = body[0],
                            name = body[1],
                            publishedOda = LocalDateTime.parse(body[2], dtfOda),
                            start = LocalDateTime.parse(body[1].substring(17, 32), dtfName),
                            stop = LocalDateTime.parse(body[1].substring(33, 48), dtfName),
                            mission = body[1].substring(0, 3),
                            dtId = body[1].substring(56, 62),
                            orbit = body[1].substring(49, 55).toInt(),
                            prodType = body[1].substring(4, 16),
                            crc = body[1].substring(63, 67)
                        )

                    if(config.upsert && productRepository.existsById(prod.name))
                        prod.apply {
                            val existing = productRepository.findById(prod.name).get()
                            publishedHub = existing.publishedHub
                            footprint = existing.footprint
                            attributes = existing.attributes
                            timeliness = existing.timeliness
                        }

                    productRepository.save(prod)
                }
    }
}