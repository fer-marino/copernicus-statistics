package com.serco.sentinel1.wamp.config

import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@Configuration
class ElasticsearchConfigurator {
    @Value("\${elasticSearch.url}") private lateinit var url: String
    @Value("\${elasticSearch.port}") private lateinit var port: String
    @Value("\${elasticSearch.scheme}") private lateinit var scheme: String
    @Value("\${elasticSearch.user}") private lateinit var user: String
    @Value("\${elasticSearch.password}") private lateinit var password: String
    @Value("\${elasticSearch.prefix}") private lateinit var prefix: String

    @Bean
    fun getElasticsearchClient(): RestHighLevelClient {
        val credentialsProvider = BasicCredentialsProvider().apply {
            setCredentials(AuthScope.ANY, UsernamePasswordCredentials(user, password))
        }

        val builder = RestClient.builder(HttpHost(url, port.toInt(), scheme))
                .setHttpClientConfigCallback { it.setDefaultCredentialsProvider(credentialsProvider) }
        if(!prefix.trim().isEmpty() && prefix.trim() != "/")
            builder.setPathPrefix(prefix)

        return RestHighLevelClient(builder)
    }

}
