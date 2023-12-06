package com.cortesrmzcau.java;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Log4j2
public class TransactionProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put("bootstrap-servers", "localhost:9092"); // Broker de kafka
        properties.put("ack", "all"); // el all es para afirmar que todos los nodos ya recibieron ese mensaje
        properties.put("transaction.id", "devs4j-producer");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try(Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties)) {
            try {
                // ProducerRecord es la clase en java que se utiliza para representar el registro que sera enviado a kafka
                producer.initTransactions();
                producer.beginTransaction();
                // Ejemplo de envio asincrono
                for (int i = 0; i < 10; i++) {
                    producer.send(new ProducerRecord<>("devs4j-topic", "devs4j-key", "devs4j-value"));
                    // Si quiero que el producer se ejecute de forma sincrona se debera agregar un .get() al final del send
                    if (i == 50000) {
                        throw new Exception("Unexpected Exception");
                    }
                }
                producer.commitTransaction();
                producer.flush(); // Flush forza el envio de registros pendientes antes de cerrar el producer
            } catch (Exception e) {
                log.error("Error", e);
                producer.abortTransaction();
            }
        }
    }
}
