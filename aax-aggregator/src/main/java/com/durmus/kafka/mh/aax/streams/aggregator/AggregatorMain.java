package com.durmus.kafka.mh.aax.streams.aggregator;

import com.durmus.kafka.mh.avro.aax.AAX;
import com.durmus.kafka.mh.avro.aax.AAXCore;
import com.durmus.kafka.mh.avro.aax.AAXEval;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.WeekFields;
import java.util.Collections;
import java.util.Properties;




public class AggregatorMain {

    private Logger log = LoggerFactory.getLogger(AggregatorMain.class.getSimpleName());
    private static Util util = new Util();

    public static void main(String[] args) {
        AggregatorMain aggregatorMain = new AggregatorMain();
        aggregatorMain.start();

    }

    private void start() {

        Properties config = getKafkaStreamsConfig();
        KafkaStreams streams = createTopology(config);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }



    private Properties getKafkaStreamsConfig() {
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, util.config.getApplicationId());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, util.config.getBootstrapServers());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
//        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        // Exactly once processing!!
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, util.config.getSchemaRegistryUrl());
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, util.config.getSchemaRegistryUrl());
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, util.config.getSchemaRegistryUrl());
        return properties;
    }

    private KafkaStreams createTopology(Properties config) {

        SpecificAvroSerde<AAX> aaxAvroSerde = new SpecificAvroSerde<>();
        aaxAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, util.config.getSchemaRegistryUrl()), false);
        SpecificAvroSerde<AAXCore> aaxCoreAvroSerde= new SpecificAvroSerde<>();
        aaxCoreAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, util.config.getSchemaRegistryUrl()), false);
        SpecificAvroSerde<AAXEval> aaxEvalAvroSerde= new SpecificAvroSerde<>();
        aaxEvalAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, util.config.getSchemaRegistryUrl()), false);



        //create topology
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, AAX> aaxFilter =
                builder.stream(util.config.getSourceTopicName(), Consumed.with(Serdes.String(), aaxAvroSerde));

        KStream<String, AAXCore> coreStream = aaxFilter
                .mapValues((k, aax) -> toCoreLayer(aax));

        KStream<String, AAXEval> evalStream = aaxFilter
                .mapValues((k, aax) -> toEvaluationLayer(aax));

        coreStream.to(util.config.getSinkCoreTopicName(), Produced.with(Serdes.String(), aaxCoreAvroSerde));
        evalStream.to(util.config.getSinkEvaluationTopicName(), Produced.with(Serdes.String(), aaxEvalAvroSerde));

        return new KafkaStreams(builder.build(), config);
    }



    private static AAXCore toCoreLayer(AAX aax)  {
        try {

            val aaxCore = AAXCore
                    .newBuilder()
                    .setId(Long.valueOf(aax.getId()))
                    .setMhkundennummer(aax.getMhkundennummer())
                    .setGuid(aax.getGuid())
                    .setCrmtkundennummer(aax.getCrmtkundennummer())
                    .setCarmenkundennummer(aax.getCarmenkundennummer())
                    .setKek(aax.getKek())
                    .setName(aax.getName())
                    .setAdresse(aax.getAdresse())
                    .setMail(aax.getMail())
                    .setZahlart(aax.getZahlart())
                    .setProduktname(aax.getProduktname())
                    .setKundigungsmodus(aax.getKundigungsmodus())
                    .setKundigungsgrund(aax.getKundigungsgrund())
                    .setVertragsnummer(Util.checkIsEmptyOrNull(aax.getVertragsnummer())
                            ? Long.valueOf(aax.getVertragsnummer())
                            : null)
                    .setProduktid(Util.checkIsEmptyOrNull(aax.getProduktid())
                            ? Long.valueOf(aax.getProduktid())
                            : null)
                    .setMvlz(Util.checkIsEmptyOrNull(aax.getMvlz())
                            ? Long.valueOf(aax.getMvlz())
                            : null)
                    .setVertragsstart(Util.getEpochMillis(aax.getVertragsstart()))
                    .setVertragsende(Util.getEpochMillis(aax.getVertragsende()))
                    .setKundigungsdatum(Util.getEpochMillis(aax.getKundigungsdatum()))
                    .setMvlzendedatum(Util.getEpochMillis(aax.getMvlzendedatum()))
                    .setKundigungsdatumbindefrist(Util.getEpochMillis(aax.getKundigungsdatumbindefrist()))
                    .setDeliveryDate(Instant.now().toEpochMilli())
                    .build();
            return aaxCore;
        }
        catch (NumberFormatException  e){
            throw new NumberFormatException();
        }
    }

    private static AAXEval toEvaluationLayer(AAX aax)  {
        try {

            val aaxEval = AAXEval
                    .newBuilder()
                    .setId(Long.valueOf(aax.getId()))
                    .setMhkundennummer(aax.getMhkundennummer())
                    .setGuid(aax.getGuid())
                    .setCrmtkundennummer(aax.getCrmtkundennummer())
                    .setCarmenkundennummer(aax.getCarmenkundennummer())
                    .setKek(aax.getKek())
                    .setName(aax.getName())
                    .setAdresse(aax.getAdresse())
                    .setMail(aax.getMail())
                    .setZahlart(aax.getZahlart())
                    .setProduktname(aax.getProduktname())
                    .setKundigungsmodus(aax.getKundigungsmodus())
                    .setKundigungsgrund(aax.getKundigungsgrund())
                    .setVertragsnummer(Util.checkIsEmptyOrNull(aax.getVertragsnummer())
                            ? Long.valueOf(aax.getVertragsnummer())
                            : null)
                    .setProduktid(Util.checkIsEmptyOrNull(aax.getProduktid())
                            ? Long.valueOf(aax.getProduktid())
                            : null)
                    .setMvlz(Util.checkIsEmptyOrNull(aax.getMvlz())
                            ? Long.valueOf(aax.getMvlz())
                            : null)
                    .setVertragsstart(Util.getEpochMillis(aax.getVertragsstart()))
                    .setVertragsende(Util.getEpochMillis(aax.getVertragsende()))
                    .setKundigungsdatum(Util.getEpochMillis(aax.getKundigungsdatum()))
                    .setMvlzendedatum(Util.getEpochMillis(aax.getMvlzendedatum()))
                    .setKundigungsdatumbindefrist(Util.getEpochMillis(aax.getKundigungsdatumbindefrist()))
                    .setGeschaeftsfall(Util.getEpochMillis(aax.getVertragsende()) == 0
                            ? "Zugang"
                            : "Wegfall")
                    .setGfDatum(Util.getEpochMillis(aax.getVertragsende()) == 0
                            ? Util.getEpochMillis(aax.getVertragsstart())
                            : Util.getEpochMillis(aax.getVertragsende()))
                    .setGfMonat((long) (Util.getEpochMillis(aax.getVertragsende()) == 0
                            ? Instant.ofEpochMilli(Util.getEpochMillis(aax.getVertragsstart())).atZone(ZoneId.systemDefault()).get(WeekFields.ISO.weekOfYear())
                            : Instant.ofEpochMilli(Util.getEpochMillis(aax.getVertragsende())).atZone(ZoneId.systemDefault()).get(WeekFields.ISO.weekOfYear())))
                    .setGfWoche((long) (Util.getEpochMillis(aax.getVertragsende()) == 0
                            ? Instant.ofEpochMilli(Util.getEpochMillis(aax.getVertragsstart())).atZone(ZoneId.systemDefault()).getMonthValue()
                            : Instant.ofEpochMilli(Util.getEpochMillis(aax.getVertragsende())).atZone(ZoneId.systemDefault()).getMonthValue()))
                    .setGfTage((long) (Util.getEpochMillis(aax.getVertragsende()) == 0
                            ? Instant.ofEpochMilli(Util.getEpochMillis(aax.getVertragsstart())).atZone(ZoneId.systemDefault()).getDayOfYear()
                            : Instant.ofEpochMilli(Util.getEpochMillis(aax.getVertragsende())).atZone(ZoneId.systemDefault()).getDayOfYear()))
                    .setDeliveryDate(Instant.now().toEpochMilli())
                    .build();

            System.out.println(aaxEval);

            return aaxEval;
        }
        catch (NumberFormatException   e){
            throw new NumberFormatException();
        }
    }


}

