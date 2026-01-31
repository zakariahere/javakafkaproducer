package com.elzakaria.kafkaproducer.lessons.lesson09_interceptors;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Metrics collecting interceptor that tracks send statistics.
 * In production, this would integrate with Micrometer, Prometheus, etc.
 */
public class MetricsInterceptor implements ProducerInterceptor<String, String> {

    // Simple in-memory metrics (use Micrometer in production)
    private static final LongAdder messagesSent = new LongAdder();
    private static final LongAdder messagesSuccess = new LongAdder();
    private static final LongAdder messagesFailed = new LongAdder();
    private static final LongAdder totalBytes = new LongAdder();
    private static final AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);
    private static final AtomicLong maxLatency = new AtomicLong(0);
    private static final LongAdder totalLatency = new LongAdder();
    private static final Map<String, LongAdder> topicCounts = new ConcurrentHashMap<>();

    // Track send time for latency calculation
    private static final Map<String, Long> sendTimes = new ConcurrentHashMap<>();

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        messagesSent.increment();

        // Track bytes
        int keySize = record.key() != null ? record.key().getBytes().length : 0;
        int valueSize = record.value() != null ? record.value().getBytes().length : 0;
        totalBytes.add(keySize + valueSize);

        // Track per topic
        topicCounts.computeIfAbsent(record.topic(), k -> new LongAdder()).increment();

        // Record send time for latency tracking
        String msgId = record.topic() + "-" + record.key() + "-" + System.nanoTime();
        sendTimes.put(msgId, System.currentTimeMillis());

        // Store msgId in header for correlation (simplified)
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            messagesSuccess.increment();

            // Calculate latency (simplified - in production, correlate properly)
            long now = System.currentTimeMillis();
            long latency = Math.max(1, now - metadata.timestamp());

            totalLatency.add(latency);
            minLatency.updateAndGet(current -> Math.min(current, latency));
            maxLatency.updateAndGet(current -> Math.max(current, latency));
        } else {
            messagesFailed.increment();
        }
    }

    @Override
    public void close() {
        // In production: flush metrics, close connections
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Initialize from config if needed
    }

    public static void resetMetrics() {
        messagesSent.reset();
        messagesSuccess.reset();
        messagesFailed.reset();
        totalBytes.reset();
        minLatency.set(Long.MAX_VALUE);
        maxLatency.set(0);
        totalLatency.reset();
        topicCounts.clear();
        sendTimes.clear();
    }

    public static void printMetrics() {
        long sent = messagesSent.sum();
        long success = messagesSuccess.sum();
        long failed = messagesFailed.sum();

        System.out.println("\n    Collected Metrics:");
        System.out.println("    ┌─────────────────────────────────────────────┐");
        System.out.println("    │ messages_sent_total:     " + String.format("%6d", sent) + "           │");
        System.out.println("    │ messages_success_total:  " + String.format("%6d", success) + "           │");
        System.out.println("    │ messages_failed_total:   " + String.format("%6d", failed) + "           │");
        System.out.println("    │ total_bytes_sent:        " + String.format("%6d", totalBytes.sum()) + "           │");
        System.out.println("    │ success_rate:            " + String.format("%5.1f%%", sent > 0 ? (100.0 * success / sent) : 0) + "           │");

        if (success > 0) {
            System.out.println("    │ avg_latency_ms:          " + String.format("%6d", totalLatency.sum() / success) + "           │");
            long min = minLatency.get();
            long max = maxLatency.get();
            if (min != Long.MAX_VALUE) {
                System.out.println("    │ min_latency_ms:          " + String.format("%6d", min) + "           │");
            }
            if (max > 0) {
                System.out.println("    │ max_latency_ms:          " + String.format("%6d", max) + "           │");
            }
        }
        System.out.println("    └─────────────────────────────────────────────┘");

        if (!topicCounts.isEmpty()) {
            System.out.println("\n    Messages per topic:");
            topicCounts.forEach((topic, count) ->
                    System.out.println("      " + topic + ": " + count.sum()));
        }
    }
}
