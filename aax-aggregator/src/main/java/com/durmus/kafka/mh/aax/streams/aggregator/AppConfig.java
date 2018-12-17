package com.durmus.kafka.mh.aax.streams.aggregator;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
class AppConfig {
    private String applicationId;
    private String bootstrapServers;
    private String schemaRegistryUrl;
    private String sourceTopicName;
    private String sinkCoreTopicName;
    private String sinkEvaluationTopicName;
    private String datePattern;
}