package com.cortesrmzcau.java;

import java.util.Properties;

public class TransactionConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put("bootstrap-servers", "localhost:9092"); // Broker de kafka
        properties.put("group.id", "devs4j-group"); // Groupid con tiene 1 o mas consumidores de mensajes
        properties.put("isolationlevel", "read_committed"); // Solo se van a leer los mensajes que esten committed
        properties.put("enable.auto.commit", "true"); // En backogrund se realiza un commit por cada mensaje que se lee
        properties.put("auto.commit.internal.ms", "1000"); // Cada que tiempo se va a realizar el commit a esos offsets
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }
}
