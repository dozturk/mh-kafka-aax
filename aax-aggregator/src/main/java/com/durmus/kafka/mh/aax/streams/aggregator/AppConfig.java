package com.durmus.kafka.mh.aax.streams.aggregator;

import com.typesafe.config.Config;
import lombok.Data;


public @Data class AppConfig {

    private final String applicationId;
    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String sourceTopicName;
    private final String sinkCoreTopicName;
    private final String sinkEvaluationTopicName;
    private final String datePattern;



    public AppConfig(Config config) {
        this.applicationId = config.getString("kafka.streams.application.id");
        this.bootstrapServers = config.getString("kafka.bootstrap.servers");
        this.schemaRegistryUrl = config.getString("kafka.schema.registry.url");
        this.sourceTopicName = config.getString("kafka.source.topic.name");
        this.sinkCoreTopicName = config.getString("kafka.sink.core.topic.name");
        this.sinkEvaluationTopicName = config.getString("kafka.sink.evaluation.topic.name");
        this.datePattern = config.getString("app.date.pattern");
    }
}