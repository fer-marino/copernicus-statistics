package com.serco.sentinel1.wamp

import com.serco.sentinel1.wamp.config.WampConfig
import com.serco.sentinel1.wamp.model.Product
import com.serco.sentinel1.wamp.model.ProductRepository
import org.apache.camel.Exchange
import org.apache.camel.builder.PredicateBuilder.and
import org.apache.camel.builder.RouteBuilder
import org.apache.camel.component.jackson.ListJacksonDataFormat
import org.elasticsearch.index.query.QueryBuilders.matchAllQuery
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.metrics.max.Max
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.web.client.RestTemplateBuilder
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.RestTemplate
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import java.util.regex.Pattern


@Component
class DhusPolling : RouteBuilder() {
    @Autowired lateinit var esTemplate: ElasticsearchTemplate
    @Autowired lateinit var productRepository: ProductRepository
    @Autowired lateinit var restTemplateBuilder: RestTemplateBuilder
    private var logger: Logger = Logger.getLogger("DhusPolling")

    private var running = AtomicBoolean(false)
    @Autowired lateinit var wampConfig: WampConfig

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
                        "&authMethod=Basic&httpClient.cookiePolicy=ignoreCookies&httpClient.authenticationPreemptive=true${wampConfig.extra}&httpClient.soTimeout=50000"))
                .process { exchange ->
                    running.set(true)
                    if((exchange.`in`.getHeader("timeStart") as Date).time - System.currentTimeMillis() > 5000)
                        logger.warning("Slow OData query. It took ${(exchange.`in`.getHeader("timeStart") as Date).time - System.currentTimeMillis()} msec")

                    val body = exchange.`in`.getBody(String::class.java)
                    val p = Pattern.compile("\\{\"d\":\\{\"results\":\\[(.*)\\]\\}\\}")
                    val m = p.matcher(body)
                    exchange.out = exchange.`in`.copy()
                    if (m.matches()) {
                        exchange.out.body = "[" + m.group(1) + "]"
                    }
                }.unmarshal(format).process { exchange ->
                    val i = AtomicInteger()
                    (exchange.`in`.body as List<Map<String, Any>>).parallelStream().forEach { entry ->
                        val productName = entry["Name"].toString()
                        val ingestionDate = Timestamp(entry["IngestionDate"].toString().substring(6, entry["IngestionDate"].toString().lastIndexOf(")")).toLong())
                        var t = System.currentTimeMillis()
                        i.incrementAndGet()
                        val attributesQueryResults = restTemplate.getForObject(((entry["Attributes"] as Map<String, Any>)["__deferred"] as Map<String, Any>)["uri"] as String, Map::class.java)

                        if(System.currentTimeMillis() - t > 1000)
                            logger.info("[$i] Slow metadata query. It took ${System.currentTimeMillis() - t} msec")

                        val attributes = ((attributesQueryResults["d"] as Map<String, Any>)["results"] as List<Map<String, String>>)
                                .map {it["Name"] to it["Value"]}.toMap()
                        val sdf = SimpleDateFormat("yyyyMMdd'T'HHmmss")
                        val p = Product(productName, sdf.parse(productName.substring(17, 32)),
                                sdf.parse(productName.substring(33, 48)), productName.substring(0, 3),
                                productName.substring(56, 62), productName.substring(49, 55).toLong(), productName.substring(4, 16),
                                attributes["Timeliness Category"], productName.substring(63, 67),
                                ingestionDate, attributes)
                        t = System.currentTimeMillis()
                        productRepository.save(p)
                        if(System.currentTimeMillis() - t > 1000)
                            logger.info("[$i] Slow elasticsearch store. It took ${System.currentTimeMillis() - t} msec")

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
                .otherwise().process { running.set(false) }

        from("timer://productListTimer?fixedRate=true&period=${wampConfig.pollInterval}").routeId("dhus-poll").autoStartup(true)
                .process { exchange ->
                    exchange.out = exchange.`in`.copy()
                    exchange.out.setHeader("datasource", "Data-HUB")

                    if(running.get()) exchange.out.setHeader("kill", true)

                    val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

                    val searchQuery = NativeSearchQueryBuilder()
                            .withQuery(matchAllQuery())
                            .addAggregation( AggregationBuilders.max("max_publication").field("publishedHub") )
                            .build()

                    var last = Date(0)
                    esTemplate.query(searchQuery, {
                        val value = it.aggregations.get<Max>("max_publication").value
                        last = Date( if(value.isFinite()) value.toLong() else 0 )
                    })

                    exchange.out.setHeader("filter", "substringof('S1', Name) and IngestionDate gt datetime'${sdf.format(last)}'")
                    exchange.out.setHeader("skip", 0)
                    exchange.out.setHeader("ingestionStart", sdf.format(last))
                }.choice().`when`(header("kill").isNotEqualTo(true))
                    .log("Polling dhus start at offset \${header.ingestionStart}").to("seda:query-dhus")

    }
}


