package com.elzakaria.kafkaconsumer.lessons.lesson05_error_handling;

import com.elzakaria.kafkaconsumer.ConsumerUtils;
import com.elzakaria.kafkaconsumer.lessons.Lesson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lesson 05: Error Handling & Resilience
 *
 * Build robust error handling and recovery patterns.
 */
@Component
public class Lesson05ConsumerErrorHandling implements Lesson {

    private static final String TOPIC = "lesson05-errors";
    private static final String DLT_TOPIC = "lesson05-errors-dlt";
    private static final String GROUP_ID = "lesson05-group";

    @Override
    public int getLessonNumber() {
        return 5;
    }

    @Override
    public String getTitle() {
        return "Error Handling - Retries and Recovery";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers error handling strategies for resilient consumers:

            1. ERROR CLASSIFICATION:
               - RETRIABLE: Transient errors (network, timeout, leader not available)
                 → Broker might recover, retry makes sense
               - NON-RETRIABLE: Permanent errors (serialization, auth, validation)
                 → Broker won't fix, retrying won't help

            2. RETRY STRATEGY:
               - Exponential backoff: Retry with increasing delays
               - Max retries: Prevent infinite retry loops
               - Max backoff time: Cap the wait duration
               - Jitter: Add randomness to prevent thundering herd

            3. DEAD LETTER TOPIC (DLT):
               - Quarantine failed messages
               - Later analysis and potential replay
               - Prevents poison pills from crashing consumer

            4. CIRCUIT BREAKER:
               - Monitor error rates
               - If too many failures, stop trying
               - Prevent cascading failures

            5. MONITORING & ALERTING:
               - Track retry count
               - Alert on high error rates
               - Monitor DLT size

            6. ANTI-PATTERNS:
               - Infinite retries (consumer hangs forever)
               - No backoff (overwhelming broker with retries)
               - Ignoring non-retriable errors
            """;
    }

    @Override
    public void run() throws Exception {
        ConsumerUtils.createTopics(2, TOPIC);

        produceMessages();

        explainErrorTypes();
        demonstrateRetryLogic();
        demonstrateDeadLetterTopic();
        demonstrateCircuitBreaker();
        demonstrateMonitoring();

        tip("Use exponential backoff with jitter for retries");
        tip("Implement Dead Letter Topic for all error scenarios");
        tip("Monitor error rates continuously");
        warning("NEVER use infinite retries - set max retry count!");
    }

    private void produceMessages() throws Exception {
        explain("Producing test messages");

        var producerProps = new HashMap<String, Object>();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps)) {
            for (int i = 1; i <= 10; i++) {
                producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "key-" + i, "Message " + i));
            }
            producer.flush();
            result("Produced 10 messages");
        }
    }

    private void explainErrorTypes() {
        explain("1. ERROR TYPES: Retriable vs Non-retriable");

        System.out.println("""
            RETRIABLE ERRORS (Consumer automatically retries):
            ┌──────────────────────────────────────────────────────┐
            │ NetworkException        → Network hiccup, retry later │
            │ LeaderNotAvailableEx    → Leader election, will fix   │
            │ OffsetsOutOfRangeEx     → Metadata lag, will fix      │
            │ TimeoutException        → Slow broker, might recover  │
            │ UnknownMemberIdEx       → Rebalancing, will fix       │
            └──────────────────────────────────────────────────────┘

            NON-RETRIABLE ERRORS (Retrying won't help):
            ┌──────────────────────────────────────────────────────┐
            │ SerializationException  → Bad data, won't get better  │
            │ OffsetMetadataTooLarge  → Data structure problem      │
            │ GroupAuthorizationEx    → Auth issue, won't fix       │
            │ GroupCoordinatorNotAvail→ Permanent service issue     │
            └──────────────────────────────────────────────────────┘
            """);

        result("Kafka consumer handles retriable errors automatically");
        result("Non-retriable errors require application handling");
    }

    private void demonstrateRetryLogic() throws Exception {
        explain("2. RETRY LOGIC: Exponential backoff with max attempts");

        step("Simulating consumer with retry logic...");

        int maxRetries = 3;
        int baseBackoffMs = 100;
        int maxBackoffMs = 10000;

        AtomicInteger attempt = new AtomicInteger(0);

        while (attempt.get() < 5) {
            try {
                attempt.incrementAndGet();
                result("Attempt #" + attempt.get());

                // Simulate processing that might fail
                if (attempt.get() < 3) {
                    throw new Exception("Transient error - broker temporarily unavailable");
                }

                success("Processing succeeded!");
                break;

            } catch (Exception e) {
                if (attempt.get() >= maxRetries) {
                    error("Max retries exceeded: " + e.getMessage());
                    break;
                }

                long backoffMs = Math.min(
                        (long) baseBackoffMs * (1L << (attempt.get() - 1)),  // Exponential: 100, 200, 400
                        maxBackoffMs
                );
                // Add jitter (random ±10%)
                long jitter = (long) (backoffMs * 0.1 * (Math.random() - 0.5));
                backoffMs = Math.max(0, backoffMs + jitter);

                result("Retrying in " + backoffMs + "ms...");
                Thread.sleep(backoffMs);
            }
        }

        success("Retry logic completed");
    }

    private void demonstrateDeadLetterTopic() throws Exception {
        explain("3. DEAD LETTER TOPIC: Quarantine failed messages");

        step("Consumer reading with DLT fallback...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-dlt");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            int successCount = 0;
            int failureCount = 0;

            for (ConsumerRecord<String, String> record : records) {
                try {
                    // Simulate processing
                    if (record.value().contains("error")) {
                        throw new Exception("Processing failed");
                    }
                    successCount++;
                    result("Processed: " + record.value());

                } catch (Exception e) {
                    failureCount++;
                    error("Failed: " + record.value() + " - sending to DLT");
                    // Send to DLT
                    sendToDLT(record);
                }
            }

            consumer.commitSync();

            result("Processed: " + successCount + " success, " + failureCount + " sent to DLT");
            success("DLT pattern prevents poison pills from crashing consumer");

            tip("DLT = separate topic for failed messages");
            tip("Use DLT for analysis, debugging, potential replay");
        }
    }

    private void sendToDLT(ConsumerRecord<String, String> record) throws Exception {
        // In production, send to actual DLT topic
        System.out.println("    [DLT] Sent to topic: " + DLT_TOPIC +
                ", offset: " + record.offset() +
                ", value: " + record.value());
    }

    private void demonstrateCircuitBreaker() throws Exception {
        explain("4. CIRCUIT BREAKER: Stop retrying on cascading failures");

        System.out.println("""
            Circuit Breaker States:
            ┌─────────────────────────────────────────────────────┐
            │ CLOSED (normal operation)                           │
            │ → Processing works, failures are low                │
            │ → Tries to process messages                         │
            │                                                     │
            │ OPEN (failure rate exceeded)                        │
            │ → Error rate > threshold (e.g., 50%)               │
            │ → Stops processing, fast-fails                      │
            │ → Prevents load on struggling system                │
            │                                                     │
            │ HALF_OPEN (recovery checking)                       │
            │ → After timeout, try a few messages                │
            │ → If successful, go back to CLOSED                │
            │ → If failed, go back to OPEN                       │
            └─────────────────────────────────────────────────────┘
            """);

        result("Example thresholds:");
        result("  - Failure threshold: 50% (5 of 10)");
        result("  - Timeout before HALF_OPEN: 30 seconds");
        result("  - Success needed to close: 2 consecutive");

        tip("Prevents thundering herd problem");
        tip("Allows system time to recover");
    }

    private void demonstrateMonitoring() throws Exception {
        explain("5. MONITORING: Track errors and health");

        System.out.println("""
            Key Metrics to Monitor:
            ┌──────────────────────────────────────────────────┐
            │ consumer-lag                                     │
            │ → Distance from end of topic (messages behind)   │
            │ → Alert if > 10000 or growing                   │
            │                                                 │
            │ error-rate                                       │
            │ → Percentage of messages that fail               │
            │ → Alert if > 1%                                │
            │                                                 │
            │ retry-count                                      │
            │ → Number of retries per record                  │
            │ → High = unstable system                        │
            │                                                 │
            │ dlt-messages                                     │
            │ → Messages sent to Dead Letter Topic            │
            │ → Indicates ongoing failures                    │
            │                                                 │
            │ processing-latency                               │
            │ → Time to process message                       │
            │ → P50, P95, P99 percentiles                     │
            └──────────────────────────────────────────────────┘
            """);

        result("Example alert rules:");
        result("  - If error_rate > 5%, page on-call");
        result("  - If dlt_messages > 100/min, escalate");
        result("  - If lag growing for 5 mins, warn");

        tip("Export metrics to Prometheus + Grafana");
        tip("Set up alerts based on business SLOs");
    }

    private Map<String, Object> createConsumerProps(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("enable.auto.commit", false);
        props.put("auto.offset.reset", "earliest");
        return props;
    }
}

