package com.windowforsun.scs.es.sink;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.expression.Expression;

@Data
@ConfigurationProperties("elasticsearch.sink")
public class ElasticsearchSinkProperties {
    private String index;
    private String dateTimeRollingFormat;
    private Expression id;
    private String routing;
    private long timeoutSeconds;
    private boolean async;
    private int batchSize = 1;
    private long groupTimeout = -1L;
}
