package com.windowforsun.scs.es.sink;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.aggregator.AggregatingMessageHandler;
import org.springframework.integration.aggregator.MessageCountReleaseStrategy;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.store.MessageGroupStore;
import org.springframework.integration.store.SimpleMessageStore;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
@Configuration
public class ElasticsearchSink {

    @Bean
    public AggregatingMessageHandler aggregator(MessageGroupStore messageGroupStore,
                                                ElasticsearchSinkProperties elasticsearchSinkProperties) {
        AggregatingMessageHandler handler = new AggregatingMessageHandler(
                group -> group.getMessages()
                        .stream()
                        .map(m -> createIndexRequest(m, elasticsearchSinkProperties))
                        .reduce(new BulkRequest(),
                                (bulk, indexRequest) -> {
                                    bulk.add(indexRequest);
                                    return bulk;
                                },
                                (bulk1, bulk2) -> {
                                    bulk1.add(bulk2.requests());

                                    return bulk1;
                                })
        );

        handler.setCorrelationStrategy(message -> "");
        handler.setReleaseStrategy(new MessageCountReleaseStrategy(elasticsearchSinkProperties.getBatchSize()));
        long groupTimeout = elasticsearchSinkProperties.getGroupTimeout();
        if (groupTimeout >= 0) {
            handler.setGroupTimeoutExpression(new ValueExpression<>(groupTimeout));
        }
        handler.setMessageStore(messageGroupStore);
        handler.setExpireGroupsUponCompletion(true);
        handler.setSendPartialResultOnExpiry(true);

        return handler;
    }

    @Bean
    public MessageGroupStore messageGroupStore() {
        SimpleMessageStore messageGroups = new SimpleMessageStore();
        messageGroups.setTimeoutOnIdle(true);
        messageGroups.setCopyOnGet(false);

        return messageGroups;
    }

    @Bean
    public IntegrationFlow elasticsearchConsumerFlow(AggregatingMessageHandler aggregator,
                                                     ElasticsearchSinkProperties elasticsearchSinkProperties,
                                                     MessageHandler bulkRequestHandler,
                                                     MessageHandler indexRequestHandler) {
        final IntegrationFlowBuilder builder = IntegrationFlows
                .from(Consumer.class, gateway -> gateway.beanName("elasticsearchConsumer"));

        int batchSize = elasticsearchSinkProperties.getBatchSize();

        if (batchSize > 1) {
            builder.handle(aggregator)
                    .handle(bulkRequestHandler);
        } else {
            builder.handle(indexRequestHandler);
        }

        return builder.get();
    }

    @Bean
    public MessageHandler bulkRequestHandler(RestHighLevelClient restHighLevelClient,
                                             ElasticsearchSinkProperties elasticsearchSinkProperties) {
        return message -> this.index(restHighLevelClient, (BulkRequest) message.getPayload(), elasticsearchSinkProperties.isAsync());
    }

    @Bean
    public MessageHandler indexRequestHandler(RestHighLevelClient restHighLevelClient, ElasticsearchSinkProperties elasticsearchSinkProperties) {
        return message -> this.index(restHighLevelClient, createIndexRequest(message, elasticsearchSinkProperties), elasticsearchSinkProperties.isAsync());
    }

    private IndexRequest createIndexRequest(Message<?> message,
                                            ElasticsearchSinkProperties elasticsearchSinkProperties) {
        IndexRequest indexRequest = new IndexRequest();
        final String INDEX_ID = "INDEX_ID";
        final String INDEX_NAME = "INDEX_NAME";

        String index = (String) message.getHeaders().getOrDefault(INDEX_NAME, elasticsearchSinkProperties.getIndex());

        String dateTimeRollingFormat = elasticsearchSinkProperties.getDateTimeRollingFormat();
        if (StringUtils.isNotEmpty(dateTimeRollingFormat)) {
            try {
                String format = LocalDateTime.now().format(DateTimeFormatter.ofPattern(dateTimeRollingFormat));
                index += "-" + format;
            } catch (Exception ignore) {

            }
        }

        indexRequest.index(index);

        String id = (String) message.getHeaders().getOrDefault(INDEX_ID, StringUtils.EMPTY);

        if (elasticsearchSinkProperties.getId() != null) {
            id = elasticsearchSinkProperties.getId().getValue(message, String.class);
        }

        indexRequest.id(id);

        Object messagePayload = message.getPayload();

        if (messagePayload instanceof String) {
            indexRequest.source((String) messagePayload, XContentType.JSON);
        } else if (messagePayload instanceof Map) {
            indexRequest.source((Map<String, ?>) messagePayload, XContentType.JSON);
        } else if (messagePayload instanceof XContentBuilder) {
            indexRequest.source((XContentBuilder) messagePayload);
        }

        String routing = elasticsearchSinkProperties.getRouting();
        if (StringUtils.isNotEmpty(routing)) {
            indexRequest.routing(routing);
        }

        long timeout = elasticsearchSinkProperties.getTimeoutSeconds();
        if (timeout > 0) {
            indexRequest.timeout(TimeValue.timeValueSeconds(timeout));
        }

        log.info("createIndexRequest index : {}, payload : {}", index, messagePayload);
        return indexRequest;
    }

    private void index(RestHighLevelClient restHighLevelClient,
                       BulkRequest request,
                       boolean isAsync) {
        Consumer<BulkResponse> handleResponse = responses -> {
            if (log.isDebugEnabled() || responses.hasFailures()) {
                for (BulkItemResponse itemResponse : responses) {
                    if (itemResponse.isFailed()) {
                        log.error(String.format("Index operation [i=%d, id=%s, index=%s] failed: %s",
                                itemResponse.getItemId(), itemResponse.getId(), itemResponse.getIndex(), itemResponse.getFailureMessage())
                        );
                    } else {
                        DocWriteResponse r = itemResponse.getResponse();
                        log.debug(String.format("Index operation [i=%d, id=%s, index=%s] succeeded: document [id=%s, version=%d] was written on shard %s.",
                                itemResponse.getItemId(), itemResponse.getId(), itemResponse.getIndex(), r.getId(), r.getVersion(), r.getShardId())
                        );
                    }
                }
            }

            if (responses.hasFailures()) {
                throw new IllegalStateException("Bulk indexing operation completed with failures: " + responses.buildFailureMessage());
            }
        };

        if (isAsync) {
            log.info("bulkRequest async document desc : {}", request.getDescription());
            restHighLevelClient.bulkAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    handleResponse.accept(bulkItemResponses);
                }

                @Override
                public void onFailure(Exception e) {
                    throw new IllegalStateException("Error occurred while performing bulk index operation: " + e.getMessage(), e);
                }
            });
        } else {
            try {
                log.info("bulkRequest document desc : {}", request.getDescription());
                BulkResponse bulkResponse = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
                handleResponse.accept(bulkResponse);
            } catch (Exception e) {
                throw new IllegalStateException("Error occurred while performing bulk index operation: " + e.getMessage(), e);
            }
        }
    }

    private void index(RestHighLevelClient restHighLevelClient,
                       IndexRequest request,
                       boolean isAsync) {
        Consumer<IndexResponse> handleResponse = response ->
                log.debug(String.format("Index operation [index=%s] succeeded: document [id=%s, version=%d] was written on shard %s.",
                        response.getIndex(), response.getId(), response.getVersion(), response.getShardId())
                );

        if (isAsync) {
            log.info("indexRequest async document desc : {}", request.getDescription());
            restHighLevelClient.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    handleResponse.accept(indexResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    throw new IllegalStateException("Error occurred while indexing document: " + e.getMessage(), e);
                }
            });
        } else {
            try {
                log.info("indexRequest document desc : {}", request.getDescription());
                IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
                handleResponse.accept(response);
            } catch (IOException e) {
                throw new IllegalStateException("Error occurred while indexing document: " + e.getMessage(), e);
            }
        }
    }

    @Getter
    @ToString
    @RequiredArgsConstructor
    static class MessageWrapper {
        private final Message<?> message;
    }
}
