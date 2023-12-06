package com.cortesrmzcau.java;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

@Log4j2
class CallBackPersonalizado implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            log.info("There was an error {} ", exception.getMessage() );
        }
        log.info("Offset = {}, Partiton = {}, Topic = {} ", metadata.offset(), metadata.partition(), metadata.topic());
    }
}

@Log4j2
public class EKafkaProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092"); // Broker de kafka
        properties.put("ack", "all"); // el all es para afirmar que todos los nodos ya recibieron ese mensaje
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        /*  StringSerializer en kafka se utiliza para serializar cadenas en un
        *   formato adecuado si no quiero usar un string en la key, y quiero usar un
        *   int se debe cambiar el stringSerializer por un intSerialaizer
        */

        /*  Si hay una clase que extiende de una clase closable el objeto se agrega en un
         *  parentesis sobre el try y ya no adentro de las {}
         */

        try(Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties)) {
            // ProducerRecord es la clase en java que se utiliza para representar el registro que sera enviado a kafka

            // Ejemplo de envio asincrono
            for (int i = 0; i < 1000; i++) {
                // Para gestionar el orden de los mensajes
                producer.send(new ProducerRecord<>("cortesrmzcau", (i % 2 == 0) ? "key-2.1" : "key-3.1", String.valueOf(i)), new CallBackPersonalizado());
                // Si quiero que el producer se ejecute de forma sincrona se debera agregar un .get() al final del send
            }

            producer.flush(); // Flush forza el envio de registros pendientes antes de cerrar el producer
        }
    }
}
