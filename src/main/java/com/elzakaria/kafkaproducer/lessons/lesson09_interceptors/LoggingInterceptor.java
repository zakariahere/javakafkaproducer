package com.elzakaria.kafkaproducer.lessons.lesson09_interceptors;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Simple logging interceptor that logs all sent messages and their outcomes.
 */
public class LoggingInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        System.out.println("    [LoggingInterceptor] onSend: topic=" + record.topic() +
                ", key=" + record.key() +
                ", headers=" + record.headers());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            System.out.println("    [LoggingInterceptor] onAck: partition=" + metadata.partition() +
                    ", offset=" + metadata.offset() +
                    ", timestamp=" + metadata.timestamp());
        } else {
            System.out.println("    [LoggingInterceptor] onAck FAILED: " + exception.getMessage());
        }
    }

    @Override
    public void close() {
        System.out.println("    [LoggingInterceptor] Closing...");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println("    [LoggingInterceptor] Configured");
    }
}
