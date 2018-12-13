package com.durmus.kafka.mh.aax.runnable;

import com.durmus.kafka.mh.aax.AppConfig;
import com.durmus.kafka.mh.avro.aax.AAX;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class AaxAvroProducerThread  implements Runnable {

    private Logger log = LoggerFactory.getLogger(AaxAvroProducerThread.class.getSimpleName());

    private final AppConfig appConfig;
    private final ArrayBlockingQueue<AAX> aaxQueue;
    private final CountDownLatch latch;
    private final KafkaProducer<String, AAX> kafkaProducer;
    private final String targetTopic;

    public AaxAvroProducerThread(AppConfig appConfig, ArrayBlockingQueue<AAX> aaxQueue, CountDownLatch latch) {
        this.appConfig = appConfig;
        this.aaxQueue = aaxQueue;
        this.latch = latch;
        this.kafkaProducer = createKafkaProducer(appConfig);
        this.targetTopic = appConfig.getTopicName();
    }

    public KafkaProducer<String, AAX> createKafkaProducer(AppConfig appConfig) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //serializer class for key that implements the kafka serializer interface, high importance
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  KafkaAvroSerializer.class.getName());   //serializer class for value that implements the kafka serializer interface, high importance
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl());
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "10"); //instead of flush this property will send the record in every millisecond
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates
        return new KafkaProducer<>(properties);
    }

    @Override
    public void run() {
        int aaxCount = 0;

        try {

            //no aax data que loaded!!!
            while (latch.getCount() > 1 || aaxQueue.size() > 0){
                AAX aax = aaxQueue.poll();
                if (aax == null) {
                    Thread.sleep(200);
                } else {
                    aaxCount += 1;
                    log.info("Sending aax data " + aaxCount + ": " + aax);
                    kafkaProducer.send(new ProducerRecord<>(targetTopic, aax.getId(), aax));
                    // sleeping to slow down the pace a bit
                    Thread.sleep(appConfig.getProducerFrequencyMs());
                }
            }
        } catch (InterruptedException e) {
            log.warn("Avro Producer interrupted");
        } finally {
            close();
        }

    }

    public void close() {
        log.info("Closing Producer");
        kafkaProducer.close();
        latch.countDown();
    }
}
