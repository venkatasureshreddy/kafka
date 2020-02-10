

package com.stream;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;



public class KafkaStreamsLiveTest {
   public static String inputTopic = "test2";
   public static String bootStrapServers = "10.17.0.45:9092";
   public static String appIdConfig = "kafka_generic_test";
   public static String offsetResetConfig ="earliest";
    public static String consumerGroupId  = "kafka_generic_test";
    public static String schemaRegistryUrl="http://10.17.0.86:8081/";


    public static void shouldTestKafkaStreams() throws InterruptedException {

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-live-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<GenericRecord, GenericRecord> kStream = builder.stream(inputTopic);
        kStream
                .foreach((key, value) -> {
                    System.out.println( value.get("type"));
                        });
                //.filter((key,value) -> key!=null);



          Topology topology = builder.build();
          KafkaStreams streams = new KafkaStreams(topology,streamsConfiguration );

        String outputTopic = "outputTopic";
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        streams.start();

        Thread.sleep(30000);
        streams.close();
    }

    private void getCommonProperties(Properties properties) {
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appIdConfig);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        //properties.put(
        //        StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        //        SendToDeadRecordQueueExceptionHandler.class.getName());
        //properties.put("to_error_topic", toErrorTopic);

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);

    }

    private static  KeyValue<GenericRecord,Model> processingLogic(
            GenericRecord key, GenericRecord value) {
        try {
            Model m = new Model();
            m.setName(value.get("name").toString());
            m.setType(value.get("type").toString());
            return new KeyValue<>(key, m);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return new KeyValue<>(null,null);
    }

    public static void main(String[] args) throws InterruptedException {
        shouldTestKafkaStreams();
    }
}


