package com.elzakaria.kafkaproducer.lessons.lesson05_error_handling;

import com.elzakaria.kafkaproducer.lessons.Lesson;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Lesson 05: Error Handling and Retries
 *
 * Learn robust error handling for Kafka producers:
 * 1. Types of producer errors
 * 2. Retry configuration
 * 3. Error callbacks
 * 4. Dead letter topic pattern
 */
@Component
public class Lesson05ErrorHandling implements Lesson {

    private static final String TOPIC = "lesson05-errors";
    private static final String DLT_TOPIC = "lesson05-errors-dlt";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Map<String, Object> baseProps;

    public Lesson05ErrorHandling(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.baseProps = new HashMap<>();
        baseProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        baseProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        baseProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

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
            This lesson covers error handling strategies:

            1. ERROR TYPES:
               - Retriable: NetworkException, LeaderNotAvailable (transient)
               - Non-retriable: SerializationException, RecordTooLarge (permanent)

            2. RETRY CONFIGURATION:
               - retries: Number of retry attempts
               - retry.backoff.ms: Wait between retries
               - delivery.timeout.ms: Total time for send + retries

            3. ERROR HANDLING PATTERNS:
               - Callbacks for logging/metrics
               - Dead Letter Topics for failed messages
               - Circuit breaker for catastrophic failures

            4. BEST PRACTICES:
               - Always handle errors, never ignore futures
               - Log failures with context
               - Monitor retry rates
            """;
    }

    @Override
    public void run() throws Exception {
        explainErrorTypes();
        demonstrateRetryConfiguration();
        demonstrateErrorCallbacks();
        demonstrateDeadLetterTopic();

        tip("Set delivery.timeout.ms >= retries * retry.backoff.ms + request.timeout.ms");
        tip("Use monitoring to track retry rates - high retries indicate problems.");
    }

    private void explainErrorTypes() {
        explain("1. ERROR TYPES: Retriable vs Non-retriable");

        System.out.println("""

            RETRIABLE ERRORS (Kafka will automatically retry):
            ┌────────────────────────────────────────────────────────────────┐
            │ NetworkException        - Temporary network issues             │
            │ LeaderNotAvailableEx    - Partition leader election in progress│
            │ NotEnoughReplicasEx     - Insufficient replicas available      │
            │ TimeoutException        - Request timed out (might retry)      │
            └────────────────────────────────────────────────────────────────┘

            NON-RETRIABLE ERRORS (immediate failure):
            ┌────────────────────────────────────────────────────────────────┐
            │ SerializationException  - Cannot serialize key/value           │
            │ RecordTooLargeException - Message exceeds max.message.bytes    │
            │ InvalidTopicException   - Invalid topic name                   │
            │ AuthorizationException  - No permission to produce             │
            └────────────────────────────────────────────────────────────────┘
            """);

        result("Kafka client handles retriable errors automatically with configured retries");
    }

    private void demonstrateRetryConfiguration() throws Exception {
        explain("2. RETRY CONFIGURATION: Fine-tuning retry behavior");

        step("Creating producer with custom retry settings...");

        Map<String, Object> retryProps = new HashMap<>(baseProps);

        // Retry configuration
        retryProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        retryProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        retryProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        retryProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        // Additional reliability settings
        retryProps.put(ProducerConfig.ACKS_CONFIG, "all");
        retryProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        System.out.println("""

            Retry Configuration:
            ┌─────────────────────────────────────────────────────────────┐
            │ retries = 3              Retry up to 3 times                │
            │ retry.backoff.ms = 100   Wait 100ms between retries         │
            │ delivery.timeout.ms =    Total time budget for delivery     │
            │   30000                  (includes all retries)             │
            │ request.timeout.ms =     Timeout for single request         │
            │   5000                                                      │
            │ acks = all               Wait for all replicas              │
            │ enable.idempotence =     Prevent duplicates from retries    │
            │   true                                                      │
            └─────────────────────────────────────────────────────────────┘
            """);

        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(retryProps);
        KafkaTemplate<String, String> retryTemplate = new KafkaTemplate<>(factory);

        // Send a message with retry configuration
        SendResult<String, String> result = retryTemplate.send(
                TOPIC, "retry-key", "Message with retry config").get();

        success("Message sent with retry configuration");
        result("Offset: " + result.getRecordMetadata().offset());

        retryTemplate.destroy();

        step("Demonstrating backoff calculation...");

        System.out.println("""

            Retry Timeline Example (with backoff):
            ┌──────────────────────────────────────────────────────────────┐
            │ t=0ms     First attempt                                      │
            │ t=100ms   Retry 1 (after 100ms backoff)                      │
            │ t=200ms   Retry 2 (after 100ms backoff)                      │
            │ t=300ms   Retry 3 (after 100ms backoff)                      │
            │ t=300ms+  Give up if delivery.timeout.ms exceeded            │
            └──────────────────────────────────────────────────────────────┘
            """);
    }

    private void demonstrateErrorCallbacks() throws Exception {
        explain("3. ERROR CALLBACKS: Handling failures gracefully");

        step("Implementing comprehensive error handling...");

        kafkaTemplate.send(TOPIC, "error-test", "Testing error handling")
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        handleSendError("error-test", "Testing error handling", exception);
                    } else {
                        result("Message delivered: offset=" + result.getRecordMetadata().offset());
                    }
                });

        waitFor(1, "callback to execute");

        step("Demonstrating exception type checking...");

        // Simulate checking different exception types
        demonstrateExceptionHandling(new TimeoutException("Simulated timeout"));
        demonstrateExceptionHandling(new RecordTooLargeException("Simulated oversized message"));
        demonstrateExceptionHandling(new SerializationException("Simulated serialization error"));

        step("Pattern: Wrapping send with error handling...");

        // Example of a wrapper method
        System.out.println("""

            Best Practice - Error Handling Wrapper:
            ┌──────────────────────────────────────────────────────────────┐
            │ public CompletableFuture<Void> safeSend(String key,         │
            │                                          String value) {     │
            │     return kafkaTemplate.send(TOPIC, key, value)            │
            │         .thenAccept(result -> {                             │
            │             metrics.recordSuccess();                         │
            │             log.debug("Sent: {}", result.offset());          │
            │         })                                                   │
            │         .exceptionally(ex -> {                               │
            │             metrics.recordFailure();                         │
            │             log.error("Failed to send: {}", ex.getMessage());│
            │             sendToDeadLetter(key, value, ex);                │
            │             return null;                                     │
            │         });                                                  │
            │ }                                                            │
            └──────────────────────────────────────────────────────────────┘
            """);
    }

    private void handleSendError(String key, String value, Throwable exception) {
        Throwable cause = exception.getCause() != null ? exception.getCause() : exception;

        System.out.println("\n    Error Handler activated:");
        System.out.println("      Key: " + key);
        System.out.println("      Exception: " + cause.getClass().getSimpleName());
        System.out.println("      Message: " + cause.getMessage());

        // In production, you would:
        // 1. Log the error with context
        // 2. Update metrics
        // 3. Possibly send to dead letter topic
        // 4. Alert if error rate exceeds threshold
    }

    private void demonstrateExceptionHandling(Exception ex) {
        System.out.println("\n    Handling " + ex.getClass().getSimpleName() + ":");

        if (ex instanceof TimeoutException) {
            System.out.println("      Action: Check broker health, network connectivity");
            System.out.println("      Recovery: May retry, message might have been delivered");
        } else if (ex instanceof RecordTooLargeException) {
            System.out.println("      Action: Reduce message size or increase max.message.bytes");
            System.out.println("      Recovery: Cannot retry without modification");
        } else if (ex instanceof SerializationException) {
            System.out.println("      Action: Fix serialization logic or data");
            System.out.println("      Recovery: Cannot retry without fixing data");
        }
    }

    private void demonstrateDeadLetterTopic() throws Exception {
        explain("4. DEAD LETTER TOPIC: Handling persistent failures");

        step("Implementing dead letter topic pattern...");

        System.out.println("""

            Dead Letter Topic Pattern:
            ┌─────────────────────────────────────────────────────────────┐
            │                                                             │
            │   Producer ──► Main Topic                                   │
            │       │                                                     │
            │       │ (on failure)                                        │
            │       ▼                                                     │
            │   DLT Handler ──► Dead Letter Topic                         │
            │                       │                                     │
            │                       ▼                                     │
            │               Manual Review / Reprocessing                  │
            │                                                             │
            └─────────────────────────────────────────────────────────────┘
            """);

        // Simulate a failed message going to DLT
        String problematicKey = "problem-message-001";
        String problematicValue = "This message failed processing";

        step("Simulating failure and sending to dead letter topic...");

        // In real scenario, this would be in the error handler
        boolean sendFailed = true; // Simulated failure

        if (sendFailed) {
            sendToDeadLetterTopic(problematicKey, problematicValue, "SimulatedError: Processing failed");
        }

        tip("Always include original message + error details in DLT messages.");
        tip("Set up alerting on DLT message count.");

        System.out.println("""

            DLT Best Practices:
            ┌─────────────────────────────────────────────────────────────┐
            │ 1. Include metadata: timestamp, error, retry count          │
            │ 2. Preserve original key for correlation                    │
            │ 3. Set longer retention for investigation                   │
            │ 4. Create tooling for reprocessing DLT messages             │
            │ 5. Alert when DLT message rate exceeds threshold            │
            └─────────────────────────────────────────────────────────────┘
            """);
    }

    private void sendToDeadLetterTopic(String originalKey, String originalValue, String errorMessage) {
        try {
            // Create DLT message with context
            String dltValue = String.format(
                    "{\"originalKey\":\"%s\",\"originalValue\":\"%s\",\"error\":\"%s\",\"timestamp\":%d}",
                    originalKey, originalValue, errorMessage, System.currentTimeMillis()
            );

            kafkaTemplate.send(DLT_TOPIC, originalKey, dltValue)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            System.out.println("    [DLT] Message sent to dead letter topic");
                            System.out.println("    [DLT] Topic: " + DLT_TOPIC);
                            System.out.println("    [DLT] Offset: " + result.getRecordMetadata().offset());
                        } else {
                            System.out.println("    [DLT] CRITICAL: Failed to send to DLT!");
                            // This is a serious issue - consider alternative storage
                        }
                    })
                    .get();
        } catch (Exception e) {
            System.out.println("    [DLT] Failed to send to dead letter topic: " + e.getMessage());
        }
    }
}
