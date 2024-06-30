package com.windowforsun.scs.es.sink;

import org.awaitility.Awaitility;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.search.SearchHit;
import org.junit.jupiter.api.BeforeAll;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@ExtendWith(SpringExtension.class)
@Import(TestChannelBinderConfiguration.class)
@Testcontainers
@SpringBootTest(
        properties = {
                "elasticsearch.sink.index=test",
                "elasticsearch.sink.batch-size=3",
                "elasticsearch.sink.group-timeout=2",
                "elasticsearch.sink.date-time-rolling-format=yyyy-MM-dd"
        }
)
@ActiveProfiles("test")
public class BatchTimeBasedIndexTest {
    private static String INDEX = "test";
    @Autowired
    private InputDestination inputDestination;
    @Autowired
    private RestHighLevelClient restHighLevelClient;
    @Autowired
    private ElasticsearchSinkProperties properties;
    @Container
    public static ElasticsearchContainer container = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.10.0");

    @DynamicPropertySource
    static void elasticsearchProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.elasticsearch.rest.uris", () -> container.getHttpHostAddress());
    }

    @BeforeEach
    public void setUp() throws IOException {
        INDEX += "-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern(this.properties.getDateTimeRollingFormat()));
    }

    @Test
    public void json_string_message_ok() throws Exception {
        int totalCount = 11;

        for(int i = 0; i < totalCount; i++) {
            this.inputDestination.send(MessageBuilder.withPayload(String.format("{\"message\" : %d}", i)).build());
        }

        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .pollDelay(100, TimeUnit.MILLISECONDS)
                .until(() -> Util.getDocumentCountByIndex(this.restHighLevelClient, INDEX), is((long) totalCount));


        List<Map<String, Integer>> documentList = Util.getAllDocumentByIndex(this.restHighLevelClient, INDEX)
                .stream()
                .map(SearchHit::getSourceAsMap)
                // List<Map<String, Object>> -> List<Map<String, Integer>>
                .map(map -> map.entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> (Integer) entry.getValue()
                        ))
                )
                .collect(Collectors.toList());

        assertThat(documentList, everyItem(hasKey("message")));
        assertThat(documentList, everyItem(hasValue(allOf(greaterThanOrEqualTo(0), lessThan(totalCount)))));
    }

    @Test
    public void map_message_ok() throws Exception {
        int totalCount = 11;

        for(int i = 0; i < totalCount; i++) {
            this.inputDestination.send(MessageBuilder.withPayload(Map.of("message", i)).build());
        }

        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .pollDelay(100, TimeUnit.MILLISECONDS)
                .until(() -> Util.getDocumentCountByIndex(this.restHighLevelClient, INDEX), is((long) totalCount));


        List<Map<String, Integer>> documentList = Util.getAllDocumentByIndex(this.restHighLevelClient, INDEX)
                .stream()
                .map(SearchHit::getSourceAsMap)
                // List<Map<String, Object>> -> List<Map<String, Integer>>
                .map(map -> map.entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> (Integer) entry.getValue()
                        ))
                )
                .collect(Collectors.toList());

        assertThat(documentList, everyItem(hasKey("message")));
        assertThat(documentList, everyItem(hasValue(allOf(greaterThanOrEqualTo(0), lessThan(totalCount)))));
    }

}
