package com.cortesrmzcau.configuration;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableScheduling
public class KafkaConfiguration {

    public Map<String, Object> consumerProperties() { //  Este método devuelve un mapa de propiedades de consumidor de Kafka.
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cortesrmzcau-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() { // Este método crea un objeto ConsumerFactory, que se utiliza para crear consumidores de Kafka
        return new DefaultKafkaConsumerFactory<>(consumerProperties());
        /* Un consumerFactory es un objeto que se utiliza para crear consumidores de Kafka.
        El consumerFactory está configurado con las propiedades necesarias para conectar
        al broker de Kafka, suscribirse a temas y deserializar los mensajes.
        */
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory() {
        /* Este método crea un objeto ConcurrentKafkaListenerContainerFactory,
        que se utiliza para crear contenedores de escucha de Kafka */
        ConcurrentKafkaListenerContainerFactory listenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerContainerFactory.setConsumerFactory(consumerFactory());
        listenerContainerFactory.setBatchListener(true);
        listenerContainerFactory.setConcurrency(3);
        return listenerContainerFactory;
    }

    public Map<String, Object> producerProperties() { //  Este método devuelve un mapa de propiedades de consumidor de Kafka.
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    @Bean // Anotación que se usa para indicarle a spring que debe crear y administrar un bean de tipo kafkaTemplate
    public KafkaTemplate<String, String> kafkaTemplate() {
        // DefaultKafkaProducerFactory se utiliza para crear y configurar contenedores de kafka
        DefaultKafkaProducerFactory<String, String> kafkaProducerFactory = new DefaultKafkaProducerFactory<>(producerProperties());
        KafkaTemplate<String, String> template = new KafkaTemplate<>(kafkaProducerFactory);
        // Configurar las metricas para el producer
        kafkaProducerFactory.addListener(new MicrometerProducerListener<>(meterRegistry()));
        return template;
    }
    // Se crea instancia de KafkaTemplate para simplificar la producción de mensajes en Kafka
    // Un bean es un objeto administrado por el contenedor de Spring que puede ser inyectado en otros componentes de la aplicación

    @Bean
    public MeterRegistry meterRegistry() {
        PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        return prometheusMeterRegistry;
    }

}
