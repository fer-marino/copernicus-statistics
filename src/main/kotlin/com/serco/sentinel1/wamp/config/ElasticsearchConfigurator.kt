package com.serco.sentinel1.wamp.config

import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@Configuration
class ElasticsearchConfigurator {
    @Autowired private lateinit var config: ElasticsearchConfig

    @Bean
    fun getElasticsearchClient(): RestHighLevelClient {
        val credentialsProvider = BasicCredentialsProvider().apply {
            setCredentials(AuthScope.ANY, UsernamePasswordCredentials(config.user, config.password))
        }

        val builder = RestClient.builder(HttpHost(config.url, config.port, config.scheme))
                .setHttpClientConfigCallback { it.setDefaultCredentialsProvider(credentialsProvider) }

        return RestHighLevelClient(builder)
    }

}

@ConfigurationProperties(prefix = "elasticSearch")
data class ElasticsearchConfig (
        var url: String = "localhost",
        var port: Int = 9200,
        var scheme: String = "http",
        var user: String = "elastic",
        var password: String = "password"
)