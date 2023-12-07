package com.cortesrmzcau.service;

import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

@Service
@Log4j2
public class KafkaService implements IKafka {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private String topico;

    @Override
    public CompletableFuture<ResponseEntity<HashMap>> enviarMensaje(String mensaje) {
        HashMap respuesta = new HashMap();

        CompletableFuture<SendResult<String, String>> future
                = kafkaTemplate.send(topico, "2", "Mensaje");

        return future.thenApply(result -> {
            log.info("Mensaje enviado al tópico: ", result.getRecordMetadata().topic());
            respuesta.put("Respuesta","Mensaje enviado");

            return ResponseEntity.ok(respuesta);
        }).exceptionally(ex -> {
            log.error("Ocurrió un error al enviar el mensaje: {}", ex.getMessage());
            respuesta.put("Respuesta", "Ocurrió un error al enviar el mensaje");

            return ResponseEntity.status(500).body(respuesta);
        });
    }
}
