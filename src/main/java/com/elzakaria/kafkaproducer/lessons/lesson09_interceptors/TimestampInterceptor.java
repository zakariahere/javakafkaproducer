package com.elzakaria.kafkaproducer.lessons.lesson09_interceptors;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

/**
 * Interceptor that adds a timestamp header to all messages.
 */
public class TimestampInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // Add timestamp header
        String timestamp = Instant.now().toString();
        record.headers().add(new RecordHeader("X-Timestamp",
                timestamp.getBytes(StandardCharsets.UTF_8)));

        System.out.println("    [TimestampInterceptor] Added X-Timestamp: " + timestamp);

        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // No action needed
    }

    @Override
    public void close() {
        // No resources to clean up
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed
    }
}
