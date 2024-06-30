package com.windowforsun.scs.es.sink;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(ElasticsearchSinkProperties.class)
public class ElasticsearchSinkApplication {
    public static void main(String... args) {
        SpringApplication.run(ElasticsearchSinkApplication.class);
    }
}
