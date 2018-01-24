package com.serco.sentinel1.wamp

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
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.regex.Pattern


@Component
class DhusPolling : RouteBuilder() {
    @Autowired lateinit var esTemplate: ElasticsearchTemplate
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
                .log("Polling \${header.address}. Skipping first \${header.skip} records")
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
                    (exchange.`in`.body as List<Map<String, Any>>).parallelStream().forEach { entry ->
//                    for (entry in exchange.`in`.body as Iterable<Map<String, Any>>) {
                        val productName = entry["Name"].toString()
                        val ingestionDate = Timestamp(entry["IngestionDate"].toString().substring(6, entry["IngestionDate"].toString().lastIndexOf(")")).toLong())

                        val attributesQueryResults = restTemplate.getForObject(((entry["Attributes"] as Map<String, Any>)["__deferred"] as Map<String, Any>)["uri"] as String, Map::class.java)
                        val attributes = ((attributesQueryResults["d"] as Map<String, Any>)["results"] as List<Map<String, String>>)
                                .map {it["Name"] to it["Value"]}.toMap()
                        val sdf = SimpleDateFormat("yyyyMMdd'T'HHmmss")
                        val p = Product(productName, sdf.parse(productName.substring(17, 32)),
                                sdf.parse(productName.substring(33, 48)), productName.substring(0, 3),
                                productName.substring(56, 62), productName.substring(49, 55).toLong(), productName.substring(4, 16),
                                attributes["Timeliness Category"], productName.substring(63, 67),
                                ingestionDate, attributes)
                        productRepository.save(p)
                    }

                    exchange.out = exchange.`in`.copy()
                    exchange.out.setHeader("productNumber", (exchange.`in`.body as List<Map<String, Any>>).size)
                })
//                .log("Polling \${header.id} complete. Received \${header.productNumber} records")
                .process({ exchange ->
                    exchange.out = exchange.`in`.copy()
                    exchange.out.body = null
                    exchange.out.setHeader("skip", exchange.out.getHeader("skip") as Int + 100)
                })
                .choice().`when`(and(header("productNumber").isEqualTo(100), header("skip").isLessThan(10000)) )
                .to("seda:query-dhus")

        from("timer://productListTimer?fixedRate=true&period=70m").routeId("dhus-poll").autoStartup(true)
                .process({ exchange ->
                    val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

                    exchange.out = exchange.`in`.copy()
                    exchange.out.setHeader("datasource", "Commissioning-HUB")

                    val searchQuery = NativeSearchQueryBuilder()
                            .withQuery(matchAllQuery())
                            .addAggregation( AggregationBuilders.max("max_publication").field("publishedHub") )
                            .build()

                    var last = Date(0)
                    esTemplate.query(searchQuery, {
                        val value = it.aggregations.get<Max>("max_publication").value
                        last = Date( if(value.isFinite()) value.toLong() else 0 )
                    })


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


