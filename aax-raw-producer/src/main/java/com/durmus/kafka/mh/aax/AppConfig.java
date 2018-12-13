package com.durmus.kafka.mh.aax;

import com.typesafe.config.Config;
import lombok.Data;

public @Data
class AppConfig {

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String topicName;
    private final Integer queueCapacity;
    private final Integer producerFrequencyMs;
    //    private final String headers;
    private final String regex;
    private final String path;


    public AppConfig(Config config) {
        this.bootstrapServers = config.getString("kafka.bootstrap.servers");
        this.schemaRegistryUrl = config.getString("kafka.schema.registry.url");
        this.topicName = config.getString("kafka.topic.name");
        this.queueCapacity = config.getInt("app.queue.capacity");
        this.producerFrequencyMs = config.getInt("app.producer.frequency.ms");
//        this.headers = config.getString("csv.headers");
        this.regex = config.getString("csv.regex");
        this.path = config.getString("csv.input.path");
    }
}