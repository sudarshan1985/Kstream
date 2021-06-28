package com.github.sudarshan.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import javax.management.MXBean;
import java.util.Arrays;
import java.util.Properties;

public class FavouriteColor {


    public  Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        // 1 - stream from Kafka

        KStream<String, String> textLines = builder.stream("favcolor-input");

        KStream<String, String> usersAndColours = textLines
        //1- We ensure that a comma is here as we will split on it
         .filter((key,value) -> value.contains(","))
        //2 - We will select user id as the key
         .selectKey((key,value) -> value.split(",")[0].toLowerCase())
        //3 - We get the color and set it as the value
          .mapValues(value -> value.split(",")[1].toLowerCase())
        //4- we filter the undesired colors
         .filter((user, colour) -> Arrays.asList("green", "blue", "red").contains(colour));

        //Write to the intermediate log compacted topic
         usersAndColours.to("userandcolor");

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        // step 2 - we read that topic as a KTable so that updates are read correctly
        KTable<String, String> usersAndColoursTable = builder.table("userandcolor");

        // step 3 - we count the occurences of colours
        KTable<String, Long> favouriteColours = usersAndColoursTable
                // 5 - we group by colour within the KTable
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColours")
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde));

        favouriteColours.toStream().to("favoutputcolor",Produced.with(Serdes.String(),Serdes.Long()));


        return builder.build();
    }


    /**
     * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
     */
    public static void main(final String[] args) {

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.5.111:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/temp2/favcolor");


        FavouriteColor favcolor = new FavouriteColor();


        KafkaStreams streams = new KafkaStreams(favcolor.createTopology(), config);
        //KafkaStreams streams = new KafkaStreams(wordCountApp.createTopology(), config);

        streams.cleanUp();
        streams.start();
        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }

}