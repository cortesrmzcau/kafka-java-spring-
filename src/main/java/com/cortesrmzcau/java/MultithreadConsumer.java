package com.cortesrmzcau.java;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultithreadConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put("bootstrap-servers", "localhost:9092"); // Broker de kafka
        properties.put("group.id", "devs4j-group"); // Groupid con tiene 1 o mas consumidores de mensajes
        properties.put("enable.auto.commit", "true"); // En backogrund se realiza un commit por cada mensaje que se lee
        properties.put("auto.commit.internal.ms", "1000"); // Cada que tiempo se va a realizar el commit a esos offsets
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        ExecutorService executorService = Executors.newFixedThreadPool(5);

        for (int i = 0; i < 10; i++) {
           ThreadConsumer consumer = new ThreadConsumer(new KafkaConsumer<>(properties));
           executorService.execute(consumer);
        }

        while(!executorService.isTerminated());
    }
}
