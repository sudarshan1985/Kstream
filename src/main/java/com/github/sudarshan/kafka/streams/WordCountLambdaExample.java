package com.github.sudarshan.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import javax.management.MXBean;
import java.util.Arrays;
import java.util.Properties;

public class WordCountLambdaExample {


    public  Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        // 1 - stream from Kafka

        KStream<String, String> textLines = builder.stream("streams-plaintext-input");
        KTable<String, Long> wordCounts = textLines
                // 2 - map values to lowercase
                .mapValues(textLine -> textLine.toLowerCase())
                // can be alternatively written as:
                // .mapValues(String::toLowerCase)
                // 3 - flatmap values split by space
                .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
                // 4 - select key to apply a key (we discard the old key)
                .selectKey((key, word) -> word)
                // 5 - group by key before aggregation
                .groupByKey()
                // 6 - count occurences
                .count(Materialized.as("Counts"));

        // 7 - to in order to write the results back to kafka
        wordCounts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }


    /**
     * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
     */
    public static void main(final String[] args) {

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.5.111:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/temp2/parallel1");


        WordCountLambdaExample wordCountApp = new WordCountLambdaExample();


        KafkaStreams streams = new KafkaStreams(wordCountApp.createTopology(), config);
        //KafkaStreams streams = new KafkaStreams(wordCountApp.createTopology(), config);

        streams.cleanUp();
        streams.start();
        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }

}