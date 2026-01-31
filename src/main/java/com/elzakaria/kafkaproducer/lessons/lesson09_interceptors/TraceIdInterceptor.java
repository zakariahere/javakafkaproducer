package com.elzakaria.kafkaproducer.lessons.lesson09_interceptors;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

/**
 * Interceptor that adds a trace ID header for distributed tracing.
 * In production, this would integrate with OpenTelemetry, Zipkin, Jaeger, etc.
 */
public class TraceIdInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // Generate trace ID (in production, use existing trace context if available)
        String traceId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);

        record.headers().add(new RecordHeader("X-Trace-Id",
                traceId.getBytes(StandardCharsets.UTF_8)));

        System.out.println("    [TraceIdInterceptor] Added X-Trace-Id: " + traceId);

        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // Could log trace completion here
    }

    @Override
    public void close() {
        // No resources to clean up
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // In production: configure trace context propagation
    }
}
