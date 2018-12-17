package com.durmus.kafka.mh.aax.streams.aggregator;

import com.durmus.kafka.mh.avro.aax.AAX;
import com.durmus.kafka.mh.avro.aax.AAXCore;
import com.durmus.kafka.mh.avro.aax.AAXEval;
import com.typesafe.config.ConfigFactory;
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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.WeekFields;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;




public class AggregatorMain {

    private Logger log = LoggerFactory.getLogger(AggregatorMain.class.getSimpleName());
    private static Util util = new Util();

    public static void main(String[] args) {
//        AggregatorMain aggregatorMain = new AggregatorMain();
//        aggregatorMain.start();

        System.out.println(Util.getSexyEpochMillis("10.10.2018"));
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
                .mapValues((k, aax) -> toCoreLayer(aax, util.config.getDatePattern()));

        KStream<String, AAXEval> evalStream = aaxFilter
                .mapValues((k, aax) -> toEvaluationLayer(aax,util.config.getDatePattern()));

        coreStream.to(util.config.getSinkCoreTopicName(), Produced.with(Serdes.String(), aaxCoreAvroSerde));
        evalStream.to(util.config.getSinkEvaluationTopicName(), Produced.with(Serdes.String(), aaxEvalAvroSerde));

        return new KafkaStreams(builder.build(), config);
    }



    private static AAXCore toCoreLayer(AAX aax, String datePattern)  {
        try {

            //val vs var?? lombok! && simple junit tests!
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





//            AAXCore.Builder AAXCoreBuilder = AAXCore.newBuilder();
//            AAXCoreBuilder.setId(Long.parseLong(aax.getId()));
//            AAXCoreBuilder.setMhkundennummer(aax.getMhkundennummer());
//            AAXCoreBuilder.setGuid(aax.getGuid());
//            AAXCoreBuilder.setCrmtkundennummer(aax.getCrmtkundennummer());
//            AAXCoreBuilder.setCarmenkundennummer(aax.getCarmenkundennummer());
//            AAXCoreBuilder.setKek(aax.getKek());
//            AAXCoreBuilder.setName(aax.getName());
//            AAXCoreBuilder.setAdresse(aax.getAdresse());
//            AAXCoreBuilder.setMail(aax.getMail());
//            AAXCoreBuilder.setZahlart(aax.getZahlart());
//            if (aax.getVertragsnummer() != null && !aax.getVertragsnummer().equals("")) {
//                AAXCoreBuilder.setVertragsnummer(Long.parseLong(aax.getVertragsnummer()));
//            } else {
//                AAXCoreBuilder.setVertragsnummer(null);
//            }
//            if (aax.getProduktid() != null && !aax.getProduktid().equals("")) {
//                AAXCoreBuilder.setProduktid(Long.parseLong(aax.getProduktid()));
//            } else {
//                AAXCoreBuilder.setProduktid(null);
//            }
//            AAXCoreBuilder.setProduktname(aax.getProduktname());
//            if (aax.getMvlz() != null && !aax.getMvlz().equals("")) {
//                AAXCoreBuilder.setMvlz(Long.parseLong(aax.getMvlz()));
//            } else {
//                AAXCoreBuilder.setMvlz(null);
//            }
//            if (aax.getVertragsstart() != null && !aax.getVertragsstart().equals("")) {
//                Date date = new SimpleDateFormat(datePattern).parse(aax.getVertragsstart());
//                Long vStart = date.toInstant().plusMillis(3600*1000*2).toEpochMilli();
//                AAXCoreBuilder.setVertragsstart(vStart);
//            } else {
//                Date date = new Date(0L);
//                Long vStart = date.toInstant().toEpochMilli();
//                AAXCoreBuilder.setVertragsstart(vStart);
//            }
//            if (aax.getVertragsende() != null && !aax.getVertragsende().equals("")) {
//                Date date = new SimpleDateFormat(datePattern).parse(aax.getVertragsende());
//                Long vEnde = date.toInstant().plusMillis(3600*1000*2).toEpochMilli();
//                AAXCoreBuilder.setVertragsende(vEnde);
//            } else {
//                Date date = new Date(0L);
//                Long vEnde = date.toInstant().toEpochMilli();
//                AAXCoreBuilder.setVertragsende(vEnde);
//            }
//            if (aax.getKundigungsdatum() != null && !aax.getKundigungsdatum().equals("")) {
//                Date date = new SimpleDateFormat(datePattern).parse(aax.getKundigungsdatum());
//                Long kDatum = date.toInstant().plusMillis(3600*1000*2).toEpochMilli();
//                AAXCoreBuilder.setKundigungsdatum(kDatum);
//            } else {
//                Date date = new Date(0L);
//                Long kDatum = date.toInstant().toEpochMilli();
//                AAXCoreBuilder.setKundigungsdatum(kDatum);
//            }
//            AAXCoreBuilder.setKundigungsmodus(aax.getKundigungsmodus());
//            AAXCoreBuilder.setKundigungsgrund(aax.getKundigungsgrund());
//            if (aax.getMvlzendedatum() != null && !aax.getMvlzendedatum().equals("")) {
//                Date date = new SimpleDateFormat(datePattern).parse(aax.getMvlzendedatum());
//                Long mDatum = date.toInstant().plusMillis(3600*1000*2).toEpochMilli();
//                AAXCoreBuilder.setMvlzendedatum(mDatum);
//            } else {
//                Date date = new Date(0L);
//                Long mDatum = date.toInstant().toEpochMilli();
//                AAXCoreBuilder.setMvlzendedatum(mDatum);
//            }
//            if (aax.getKundigungsdatumbindefrist() != null && !aax.getKundigungsdatumbindefrist().equals("")) {
//                Date date = new SimpleDateFormat(datePattern).parse(aax.getMvlzendedatum());
//                Long kDatumFrist = date.toInstant().plusMillis(3600*1000*2).toEpochMilli();
//                AAXCoreBuilder.setKundigungsdatumbindefrist(kDatumFrist);
//            } else {
//                Date date = new Date(0L);
//                Long kDatumFrist = date.toInstant().toEpochMilli();
//                AAXCoreBuilder.setKundigungsdatumbindefrist(kDatumFrist);
//            }
//            AAXCoreBuilder.setDeliveryDate(Instant.now().toEpochMilli());

//            AAXCore aaxCore = AAXCoreBuilder.build();

//            System.out.println(aaxCore);

            return aaxCore;
        }
        catch (NumberFormatException  e){
            throw new NumberFormatException();
        }
    }

    private static AAXEval toEvaluationLayer(AAX aax, String datePattern)  {
        try {
            AAXEval.Builder AAXEvalBuilder = AAXEval.newBuilder();
            AAXEvalBuilder.setId(Long.parseLong(aax.getId()));
            AAXEvalBuilder.setMhkundennummer(aax.getMhkundennummer());
            AAXEvalBuilder.setGuid(aax.getGuid());
            AAXEvalBuilder.setCrmtkundennummer(aax.getCrmtkundennummer());
            AAXEvalBuilder.setCarmenkundennummer(aax.getCarmenkundennummer());
            AAXEvalBuilder.setKek(aax.getKek());
            AAXEvalBuilder.setName(aax.getName());
            AAXEvalBuilder.setAdresse(aax.getAdresse());
            AAXEvalBuilder.setMail(aax.getMail());
            AAXEvalBuilder.setZahlart(aax.getZahlart());
            if (aax.getVertragsnummer() != null && !aax.getVertragsnummer().equals("")) {
                AAXEvalBuilder.setVertragsnummer(Long.parseLong(aax.getVertragsnummer()));
            } else {
                AAXEvalBuilder.setVertragsnummer(null);
            }
            if (aax.getProduktid() != null && !aax.getProduktid().equals("")) {
                AAXEvalBuilder.setProduktid(Long.parseLong(aax.getProduktid()));
            } else {
                AAXEvalBuilder.setProduktid(null);
            }
            AAXEvalBuilder.setProduktname(aax.getProduktname());
            if (aax.getMvlz() != null && !aax.getMvlz().equals("")) {
                AAXEvalBuilder.setMvlz(Long.parseLong(aax.getMvlz()));
            } else {
                AAXEvalBuilder.setMvlz(null);
            }
            if (aax.getVertragsstart() != null && !aax.getVertragsstart().equals("")) {
                Date date = new SimpleDateFormat(datePattern).parse(aax.getVertragsstart());
                Long vStart = date.toInstant().plusMillis(3600*1000*2).toEpochMilli();
                AAXEvalBuilder.setVertragsstart(vStart);
            } else {
                Date date = new Date(0L);
                Long vStart = date.toInstant().toEpochMilli();
                AAXEvalBuilder.setVertragsstart(vStart);
            }
            if (aax.getVertragsende() != null && !aax.getVertragsende().equals("")) {
                Date date = new SimpleDateFormat(datePattern).parse(aax.getVertragsende());
                Long vEnde = date.toInstant().plusMillis(3600*1000*2).toEpochMilli();
                AAXEvalBuilder.setVertragsende(vEnde);
            } else {
                Date date = new Date(0L);
                Long vEnde = date.toInstant().toEpochMilli();
                AAXEvalBuilder.setVertragsende(vEnde);
            }
            if (aax.getKundigungsdatum() != null && !aax.getKundigungsdatum().equals("")) {
                Date date = new SimpleDateFormat(datePattern).parse(aax.getKundigungsdatum());
                Long kDatum = date.toInstant().plusMillis(3600*1000*2).toEpochMilli();
                AAXEvalBuilder.setKundigungsdatum(kDatum);
            } else {
                Date date = new Date(0L);
                Long kDatum = date.toInstant().toEpochMilli();
                AAXEvalBuilder.setKundigungsdatum(kDatum);
            }
            AAXEvalBuilder.setKundigungsmodus(aax.getKundigungsmodus());
            AAXEvalBuilder.setKundigungsgrund(aax.getKundigungsgrund());
            if (aax.getMvlzendedatum() != null && !aax.getMvlzendedatum().equals("")) {
                Date date = new SimpleDateFormat(datePattern).parse(aax.getMvlzendedatum());
                Long mDatum = date.toInstant().plusMillis(3600*1000*2).toEpochMilli();
                AAXEvalBuilder.setMvlzendedatum(mDatum);
            } else {
                Date date = new Date(0L);
                Long mDatum = date.toInstant().toEpochMilli();
                AAXEvalBuilder.setMvlzendedatum(mDatum);
            }
            if (aax.getKundigungsdatumbindefrist() != null && !aax.getKundigungsdatumbindefrist().equals("")) {
                Date date = new SimpleDateFormat(datePattern).parse(aax.getMvlzendedatum());
                Long kDatumFrist = date.toInstant().plusMillis(3600*1000*2).toEpochMilli();
                AAXEvalBuilder.setKundigungsdatumbindefrist(kDatumFrist);
            } else {
                Date date = new Date(0L);
                Long kDatumFrist = date.toInstant().toEpochMilli();
                AAXEvalBuilder.setKundigungsdatumbindefrist(kDatumFrist);
            }

            AAXEvalBuilder.setDeliveryDate(Instant.now().toEpochMilli());

            if (AAXEvalBuilder.getVertragsende() == 0 ) {
                AAXEvalBuilder.setGeschaeftsfall("Zugang");
                AAXEvalBuilder.setGfDatum(AAXEvalBuilder.getVertragsstart());
                AAXEvalBuilder.setGfWoche((long) Instant.ofEpochMilli(AAXEvalBuilder.getVertragsstart()).atZone(ZoneId.systemDefault()).get(WeekFields.ISO.weekOfYear()));
                AAXEvalBuilder.setGfMonat((long) Instant.ofEpochMilli(AAXEvalBuilder.getVertragsstart()).atZone(ZoneId.systemDefault()).getMonthValue());
                AAXEvalBuilder.setGfTage((long) Instant.ofEpochMilli(AAXEvalBuilder.getVertragsstart()).atZone(ZoneId.systemDefault()).getDayOfYear());
            }
            else {
                AAXEvalBuilder.setGeschaeftsfall("Wegfall");
                AAXEvalBuilder.setGfDatum(AAXEvalBuilder.getVertragsende());
                AAXEvalBuilder.setGfWoche((long) Instant.ofEpochMilli(AAXEvalBuilder.getVertragsende()).atZone(ZoneId.systemDefault()).get(WeekFields.ISO.weekOfYear()));
                AAXEvalBuilder.setGfMonat((long) Instant.ofEpochMilli(AAXEvalBuilder.getVertragsende()).atZone(ZoneId.systemDefault()).getMonthValue());
                AAXEvalBuilder.setGfTage((long) Instant.ofEpochMilli(AAXEvalBuilder.getVertragsende()).atZone(ZoneId.systemDefault()).getDayOfYear());
            }


            AAXEval aaxEval = AAXEvalBuilder.build();

            System.out.println(aaxEval);
            System.out.println(Instant.ofEpochMilli(aaxEval.getVertragsstart()));

            return aaxEval;
        }
        catch (NumberFormatException | ParseException e){
            throw new NumberFormatException();
        }
    }


}

