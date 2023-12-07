package com.cortesrmzcau.controller;

import com.cortesrmzcau.service.IKafka;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

@RestController
@Log4j2
public class ControllerKafka {

    @Autowired
    private MeterRegistry meterRegistry;
    // Habilitar consumer en tiempo de ejecucion

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    private final IKafka iKafka;

    public ControllerKafka(IKafka iKafka) {
        this.iKafka = iKafka;
    }

    // Spring
    @GetMapping
    public CompletableFuture<ResponseEntity<HashMap>> enviaMensaje() {
        return iKafka.enviarMensaje("Mensaje de prueba");
    }

    // Producer
    @GetMapping(value = "/producer")
    public void producer() {
        // Enviar mensajes de forma sencilla
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send("cortesrmzcau", String.valueOf(i), String.format("Mensaje desde el controlador " + i));
            //log.info("Número Enviado: " + i);
        }
    }

    // Omitir el scheduler en caso de no usarlo
    /*@Scheduled(fixedDelay = 1000, initialDelay = 100)
    @GetMapping(value = "scheduled")
    public void print() {
        log.info("Mensaje enviado desde el scheduled");
        for(int i = 0; i < 100; i++) {
            kafkaTemplate.send("cortesrmzcau", String.valueOf(i), String.format("Sample message %d",i));
        }
    }

    @Scheduled(fixedDelay = 1000, initialDelay = 500)
    private void contador() {
        double count = meterRegistry.get("kafka.producer.record.send.total")
                .functionCounter().count();
        log.info("Mensajes actuales {}", count);
    }*/
}
