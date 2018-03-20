package com.serco.sentinel1.wamp

import com.esri.core.geometry.*
import com.serco.sentinel1.wamp.config.WampConfig
import com.serco.sentinel1.wamp.model.Product
import com.serco.sentinel1.wamp.model.ProductRepository
import org.apache.camel.Exchange
import org.apache.camel.builder.PredicateBuilder.and
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.component.jackson.ListJacksonDataFormat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.web.client.RestTemplateBuilder
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.RestTemplate
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import java.util.regex.Pattern


@Component
class DhusPolling : RouteBuilder() {
    @Autowired lateinit var restTemplateBuilder: RestTemplateBuilder
    @Autowired lateinit var wampConfig: WampConfig

    @Autowired lateinit var productRepository: ProductRepository

    private var running = AtomicBoolean(false)
    private var logger: Logger = Logger.getLogger("DhusPolling")

    private val restTemplate: RestTemplate by lazy {
        restTemplateBuilder.requestFactory(SimpleClientHttpRequestFactory::class.java)
                .basicAuthorization(wampConfig.dhusUser, wampConfig.dhusPassword)
                .setConnectTimeout(wampConfig.connectionTimeout)
                .setReadTimeout(wampConfig.readTimeout)
                .build()
    }

    @Throws(Exception::class)
    override fun configure() {
        val format = ListJacksonDataFormat()
        format.useList()

        errorHandler(deadLetterChannel("direct:dead"))

        from("direct:dead").id("Dead letter channel").process {
            val ex = if (it.exception == null) it.properties["CamelExceptionCaught"] as Exception else it.exception
            running.set(false)
            logger.severe(ex.message)
        }

        from("seda:query-dhus").routeId("query-dhus")
                .setHeader(Exchange.HTTP_URI, simple("${wampConfig.dhusUrl}/odata/v1/Products?\$orderby=IngestionDate asc"
                        + "&\$format=json&\$top=100&\$skip=\${header.skip}&\$filter=\${header.filter}"))
                .log("Polling ${wampConfig.dhusUrl}. Skipping first \${header.skip} records")
                .setHeader("timeStart", simple("\${date:now}", Date::class.java))
                .recipientList(simple("http://dummy?authUsername=${wampConfig.dhusUser}&authPassword=${wampConfig.dhusUser}" +
                        "&authMethod=Basic&httpClient.cookiePolicy=ignoreCookies&httpClient.authenticationPreemptive=true${wampConfig.extra}&httpClient.soTimeout=500000"))
                .process { exchange ->
                    running.set(true)
                    if((exchange.`in`.getHeader("timeStart") as Date).time - System.currentTimeMillis() > 5000)
                        logger.warning("Slow OData query. It took ${(exchange.`in`.getHeader("timeStart") as Date).time - System.currentTimeMillis()} msec")

                    val body = exchange.`in`.getBody(String::class.java)
                    val m = Pattern.compile("\\{\"d\":\\{\"results\":\\[(.*)\\]\\}\\}").matcher(body)
                    exchange.out = exchange.`in`.copy()
                    if (m.matches()) {
                        exchange.out.body = "[" + m.group(1) + "]"
                    }
                }.unmarshal(format).process { exchange ->
                    val bulkRequest = mutableListOf<Product>()
                    (exchange.`in`.body as List<Map<String, Any>>).parallelStream().forEach { entry ->
                        val productName = entry["Name"].toString()
                        val ingestionDate = Timestamp(entry["IngestionDate"].toString().substring(6, entry["IngestionDate"].toString().lastIndexOf(")")).toLong())
                        val t = System.currentTimeMillis()
                        var doFetch = true
                        if(wampConfig.upsert) {
                            val exist = productRepository.existsById(productName)
                            if(exist)
                                doFetch = false
                        }

                        if(doFetch) {
                            val attributesQueryResults = restTemplate.getForObject(((entry["Attributes"] as Map<String, Any>)["__deferred"] as Map<String, Any>)["uri"] as String, Map::class.java)

                            if (System.currentTimeMillis() - t > 5000)
                                logger.info("Slow metadata query for $productName. It took ${System.currentTimeMillis() - t} msec")

                            val attributes = ((attributesQueryResults!!["d"] as Map<String, Any>)["results"] as List<Map<String, String>>)
                                    .map { it["Name"] to it["Value"] }.toMap()
                            val sdf = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")

                            var geometry = OperatorImportFromWkt.local().execute(WktImportFlags.wktImportDefaults,
                                    Geometry.Type.Polygon,
                                    attributes["JTS footprint"]!!,
                                    null)
                            geometry = OperatorSimplifyOGC.local().execute(geometry, SpatialReference.create(4326), true, null)
                            val geoJson = OperatorExportToGeoJson.local().execute(geometry)

                            val prod = Product(productName,
                                    LocalDateTime.parse(productName.substring(17, 32), sdf),
                                    LocalDateTime.parse(productName.substring(33, 48), sdf),
                                    productName.substring(0, 3),
                                    productName.substring(56, 62),
                                    productName.substring(49, 55).toInt(),
                                    productName.substring(4, 16),
                                    attributes["Timeliness Category"],
                                    productName.substring(63, 67),
                                    geoJson,
                                    ingestionDate.toLocalDateTime(),
                                    null,
                                    null, null,
                                    attributes)

                            synchronized(bulkRequest, { bulkRequest.add(prod) })
                        }
                    }

                    if(bulkRequest.size > 0)
                        productRepository.saveAll(bulkRequest)
                    else
                        logger.info("Nothing to do")

                    exchange.out = exchange.`in`.copy()
                    exchange.out.setHeader("productNumber", (exchange.`in`.body as List<Map<String, Any>>).size)
                }
                .process { exchange ->
                    exchange.out = exchange.`in`.copy()
                    exchange.out.body = null
                    exchange.out.setHeader("skip", exchange.out.getHeader("skip") as Int + 100)
                }
                .choice().`when`(and(header("productNumber").isEqualTo(100), header("skip").isLessThan(10000)) )
                    .to("seda:query-dhus")
                .otherwise().process {
                    running.set(false)
                }

        from("timer://productListTimer?fixedRate=true&period=${wampConfig.pollInterval}").routeId("dhus-poll").autoStartup(wampConfig.autoStart)
                .process { exchange ->
                    exchange.out = exchange.`in`.copy()
                    exchange.out.setHeader("datasource", "Data-HUB")

                    if(running.get()) exchange.out.setHeader("kill", true)

                    val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

                    var last = Date(0)
                    if(!wampConfig.reindex) {
                        val value = productRepository.getMaxPublishedHub()

                        last = Date(value?.atZone(ZoneId.of("UTC"))?.toEpochSecond() ?: 0)
                    }

                    exchange.out.setHeader("filter", "substringof('S1', Name) and IngestionDate gt datetime'${sdf.format(last)}'")
                    exchange.out.setHeader("skip", 0)
                    exchange.out.setHeader("ingestionStart", sdf.format(last))
                }.choice().`when`(header("kill").isNotEqualTo(true))
                    .log("Polling dhus start at offset \${header.ingestionStart}").to("seda:query-dhus")

    }

}


