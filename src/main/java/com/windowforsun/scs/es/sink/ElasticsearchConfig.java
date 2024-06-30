package com.windowforsun.scs.es.sink;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {
    @Value("${spring.elasticsearch.rest.uris}")
    private String elasticsearchUris;
    @Value("${spring.elasticsearch.rest.username}")
    private String elasticsearchUsername;
    @Value("${spring.elasticsearch.rest.password}")
    private String elasticsearchPassword;

    @Bean
    public RestHighLevelClient client() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(this.elasticsearchUsername, this.elasticsearchPassword));

        RestClientBuilder builder = RestClient.builder(HttpHost.create(this.elasticsearchUris))
                .setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }
}
