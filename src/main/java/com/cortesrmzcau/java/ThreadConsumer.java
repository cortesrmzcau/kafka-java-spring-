package com.cortesrmzcau.java;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public class ThreadConsumer extends Thread {
    private final KafkaConsumer<String, String> consumer;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ThreadConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList("devs4j-topic"));

        try {
            while(!closed.get()) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.debug("Offset = {}, Partition = {}, Key = {}, Value = {} ",
                            consumerRecord.offset(),
                            consumerRecord.partition(),
                            consumerRecord.key(),
                            consumerRecord.value());

                    if ((Integer.parseInt(consumerRecord.key()) % 100000) == 0) {
                        // Para evitar que se tarde en publicar, se imprime el resultado cada 100,000 registros
                        log.info("Offset = {}, Partition = {}, Key = {}, Value = {} ",
                                consumerRecord.offset(),
                                consumerRecord.partition(),
                                consumerRecord.key(),
                                consumerRecord.value());
                    }
                }
            }
        } catch(WakeupException wakeupException) {
            if (!closed.get()) {
                throw wakeupException;
            }
        } finally {
            consumer.close();
        }

    }

    public void shutDown() {
        closed.set(true);
        consumer.wakeup();
    }
}
