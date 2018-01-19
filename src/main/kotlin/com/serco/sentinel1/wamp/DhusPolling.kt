package com.serco.sentinel1.wamp

import com.datastax.driver.core.utils.UUIDs
import org.apache.camel.Exchange
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.component.jackson.ListJacksonDataFormat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.web.client.RestTemplateBuilder
import org.springframework.data.cassandra.core.CassandraTemplate
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.RestTemplate
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.regex.Pattern

@Component
class DhusPolling : RouteBuilder() {
    @Autowired lateinit var cassandraTemplate: CassandraTemplate
    @Autowired lateinit var productRepository: ProductRepository
    @Autowired lateinit var restTemplateBuilder: RestTemplateBuilder
//    @Autowired lateinit var wampConfig: WampConfig

    private val restTemplate: RestTemplate by lazy {
        restTemplateBuilder.requestFactory(SimpleClientHttpRequestFactory::class.java)
                .basicAuthorization("test", "test").build()
    }

    private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")

    @Throws(Exception::class)
    override fun configure() {

        val format = ListJacksonDataFormat()
        format.useList()

        from("seda:query-dhus").routeId("query-dhus")
                .setHeader(Exchange.HTTP_URI, simple("\${header.address}/odata/v1/Products?\$orderby=IngestionDate asc"
                        + "&\$format=json&\$top=100&\$skip=\${header.skip}&\$filter=\${header.filter}"))
                .log("Polling \${header.address} for \${header.id}. Skipping \${header.skip}")
                .recipientList(simple("http://dummy?authUsername=\${header.username}&authPassword=\${header.password}" +
                        "&authMethod=Basic&httpClient.cookiePolicy=ignoreCookies&httpClient.authenticationPreemptive=true\${header.extra}&httpClient.soTimeout=500000"))
                .process({ exchange ->
                    val body = exchange.`in`.getBody(String::class.java)
                    val p = Pattern.compile("\\{\"d\":\\{\"results\":\\[(.*)\\]\\}\\}")
                    val m = p.matcher(body)
                    exchange.out = exchange.`in`.copy()
                    if (m.matches()) {
                        exchange.out.body = "[" + m.group(1) + "]"
                    }
                }).unmarshal(format).process({ exchange ->
                    val hubId = exchange.`in`.getHeader("id").toString()
                    for (entry in exchange.`in`.body as Iterable<Map<String, Any>>) {
                        val productName = entry["Name"].toString()
                        val ingestionDate = Timestamp(entry["IngestionDate"].toString().substring(6, entry["IngestionDate"].toString().lastIndexOf(")")).toLong()).toLocalDateTime()

                        val attributesQueryResults = restTemplate.getForObject(((entry["Attributes"] as Map<String, Any>)["__deferred"] as Map<String, Any>)["uri"] as String, Map::class.java)
                        val attributes = ((attributesQueryResults["d"] as Map<String, Any>)["results"] as List<Map<String, String>>)
                                .map {it["Name"] to it["Value"]}.toMap()

                        val p = Product(UUIDs.timeBased(), productName, LocalDateTime.parse(productName.substring(17, 32), formatter),
                                LocalDateTime.parse(productName.substring(33, 48), formatter), productName.substring(0, 3),
                                productName.substring(56, 62), productName.substring(49, 55).toLong(),  productName.substring(4, 16),
                                attributes["Timeliness Category"], productName.substring(63, 67),
                                ingestionDate, attributes)
                        productRepository.save(p)

                    }

                    exchange.out = exchange.`in`.copy()
                    exchange.out.setHeader("productNumber", (exchange.`in`.body as List<Map<String, Any>>).size)
                }).log("Polling \${header.id} complete. Received \${header.productNumber} records")
                        .to("seda:dhus-check-product")
                        .process({ exchange ->
                            exchange.out = exchange.`in`.copy()
                            exchange.out.body = null
                            exchange.out.setHeader("skip", exchange.out.getHeader("skip") as Int + 100)
                        })
                        .choice().`when`(header("productNumber").isEqualTo(100))
                        .to("seda:query-dhus")

        from("timer://productListTimer?fixedRate=true&period=300m").routeId("dhus-poll").autoStartup(false)
                .process({ exchange ->
                    val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

                    exchange.out = exchange.`in`.copy()
                    exchange.out.setHeader("datasource", "Commissioning-HUB")
                    val ris = cassandraTemplate.select("SELECT max(publishedhub) AS last FROM product", Date::class.java)

                    val last = if (ris.isEmpty()) Date(0) else ris[0]

                    exchange.out.setHeader("address", "https://scihub.copernicus.eu/dhus/")
                    exchange.out.setHeader("id", "scihub")
                    exchange.out.setHeader("username", "test")
                    exchange.out.setHeader("password", "test")
                    exchange.out.setHeader("extra", "")
                    exchange.out.setHeader("filter", "substringof('S1', Name) and IngestionDate gt datetime'${sdf.format(last)}'")
                    exchange.out.setHeader("skip", 0)
                    exchange.out.setHeader("ingestionStart", sdf.format(last))
                }).log("Polling dhus start at offset \${header.ingestionStart}").to("seda:query-dhus")

    }
}
