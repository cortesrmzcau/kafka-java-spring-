package com.cortesrmzcau.service;

import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public interface IKafka {
    CompletableFuture<ResponseEntity<HashMap>> enviarMensaje(String mensaje);
}
