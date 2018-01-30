package com.serco.sentinel1.wamp.config

import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@Configuration
class ElasticsearchConfig {

    @Bean
    fun getElasticsearchClient(): RestHighLevelClient {
        val credentialsProvider = BasicCredentialsProvider()
        credentialsProvider.setCredentials(AuthScope.ANY, UsernamePasswordCredentials("user", "password"))

        val builder = RestClient.builder(HttpHost("localhost", 9200, "http"))
                .setHttpClientConfigCallback { it.setDefaultCredentialsProvider(credentialsProvider) }

        return RestHighLevelClient(builder)
    }

}