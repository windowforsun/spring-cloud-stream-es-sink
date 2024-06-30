package com.windowforsun.scs.es.sink;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@ExtendWith(SpringExtension.class)
@Import(TestChannelBinderConfiguration.class)
@Testcontainers
@SpringBootTest(
        properties = {
                "elasticsearch.sink.index=test",
                "elasticsearch.sink.batch-size=3",
                "elasticsearch.sink.group-timeout=2"
        }
)
@ActiveProfiles("test")
public class ExamTest {
    @Autowired
    private InputDestination inputDestination;
    @Autowired
    private RestHighLevelClient restHighLevelClient;
    @Container
//    public static ElasticsearchContainer container = new ElasticsearchContainer("arm64v8/elasticsearch:7.10.0");
    public static ElasticsearchContainer container = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.10.0");

    @DynamicPropertySource
    static void elasticsearchProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.elasticsearch.rest.uris", () -> container.getHttpHostAddress());
    }

    @BeforeEach
    public void init() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("test");
        CreateIndexResponse createIndexResponse = this.restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

        System.out.println("tes !! createIndex " + createIndexResponse.isAcknowledged());
    }

    @Test
    public void ekekekek() throws IOException, InterruptedException {
        int totalCount = 11;

        for(int i = 0; i < totalCount; i++) {
            this.inputDestination.send(MessageBuilder.withPayload(Map.of("key", String.valueOf(i))).build());

        }
//        this.inputDestination.send(MessageBuilder.withPayload(Map.of("key", "value")).build());
//        this.inputDestination.send(MessageBuilder.withPayload(Map.of("key", "value")).build());
//        this.inputDestination.send(MessageBuilder.withPayload(Map.of("key", "value")).build());
//        this.inputDestination.send(MessageBuilder.withPayload(Map.of("key", "value")).build());
//        this.inputDestination.send(MessageBuilder.withPayload(Map.of("key", "value")).build());
//        this.inputDestination.send(MessageBuilder.withPayload(Map.of("key", "value")).build());
//        this.inputDestination.send(MessageBuilder.withPayload(Map.of("key", "value")).build());
//        this.inputDestination.send(MessageBuilder.withPayload(Map.of("key", "value")).build());
//        this.inputDestination.send(MessageBuilder.withPayload(Map.of("key", "value")).build());
//        this.inputDestination.send(MessageBuilder.withPayload(Map.of("key", "value")).build());


        TimeUnit.SECONDS.sleep(3);

        CountRequest countRequest = new CountRequest("test");
        CountResponse countResponse = this.restHighLevelClient.count(countRequest, RequestOptions.DEFAULT);

        System.out.println("test !! " + countResponse.getCount());

        SearchRequest searchRequest = new SearchRequest("test");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
//        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = this.restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println("test !! " + searchResponse.getHits().getTotalHits());
        Arrays.stream(searchResponse.getHits().getHits()).forEach(System.out::println);
        System.out.println("test !! " + container.getHttpHostAddress());


    }
}
