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
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import java.util.regex.Pattern


@Component
class DhusPolling : RouteBuilder() {
    @Autowired lateinit var restTemplateBuilder: RestTemplateBuilder
    @Autowired lateinit var wampConfig: WampConfig

    @Autowired lateinit var productRepository: ProductRepository

    private var running = AtomicBoolean(false)
    private var counter = AtomicInteger(0)
    private var lastCount = AtomicInteger(0)
    private var lastTime = AtomicLong(0)

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
                //.log("Polling ${wampConfig.dhusUrl}. Skipping first \${header.skip} records")
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
                    (exchange.`in`.body as List<Map<String, Any>>).parallelStream().forEach { entry ->
                        val productName = entry["Name"].toString()
                        val ingestionDate = Timestamp(entry["IngestionDate"].toString().substring(6, entry["IngestionDate"].toString().lastIndexOf(")")).toLong())
                        var doFetch = true
                        if(wampConfig.upsert) {
                            val exist = productRepository.existsById(productName)
                            if(exist)
                                doFetch = false
                        }

                        if(doFetch) {
                            val sdf = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")

                            val prod = Product("$productName.SAFE",
                                    LocalDateTime.parse(productName.substring(17, 32), sdf),
                                    LocalDateTime.parse(productName.substring(33, 48), sdf),
                                    productName.substring(0, 3),
                                    productName.substring(56, 62),
                                    productName.substring(49, 55).toInt(),
                                    productName.substring(4, 16), null,
                                    productName.substring(63, 67), null,
                                    ingestionDate.toLocalDateTime()
                                    )

                            productRepository.save(prod)
                            counter.incrementAndGet()
                        }


                    }

                    if(System.currentTimeMillis() - lastTime.get() > 60000) {
                        log.info("Ingested $counter products with an ingestor rate of ${(counter.get() - lastCount.get()) / (System.currentTimeMillis()/1000 - lastTime.get()/1000)} prod/sec")
                        lastTime.set(System.currentTimeMillis())
                        lastCount.set(counter.get())
                    }

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
                    log.info("Scanning completed completed in ${(System.currentTimeMillis() - it.`in`.headers["processingStart"] as Long)/1000} sec")
                    running.set(false)
                }

        from("timer://productListTimer?fixedRate=true&period=${wampConfig.pollInterval}").routeId("dhus-poll").autoStartup(wampConfig.autoStart)
                .process { exchange ->
                    exchange.out = exchange.`in`.copy()
                    exchange.out.setHeader("datasource", "Data-HUB")

                    if(running.get()) exchange.out.setHeader("kill", true)

                    val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

                    val origin = if(wampConfig.dhusIngestedFrom.isNotBlank()) sdf.parse(wampConfig.dhusIngestedFrom).time else 0
                    val last = if(!wampConfig.reindex) {
                        val max = productRepository.getMaxPublishedHub()?.atZone(ZoneId.of("UTC"))?.toEpochSecond()?.let { Math.max(origin, it*1000) }

                        if(max != null)
                            Date(max - 3600*12*1000)
                        else
                            Date(origin)
                    } else
                        Date(origin)

                    exchange.out.setHeader("filter", "substringof('S1', Name) and IngestionDate gt datetime'${sdf.format(last)}'")
                    exchange.out.setHeader("skip", 0)
                    exchange.out.setHeader("ingestionStart", sdf.format(last))
                    exchange.out.setHeader("processingStart", System.currentTimeMillis())
                }.choice().`when`(header("kill").isNotEqualTo(true))
                    .log("Polling dhus start at offset \${header.ingestionStart}").to("seda:query-dhus")
                    .process { lastTime.set(System.currentTimeMillis()) }

    }

}


