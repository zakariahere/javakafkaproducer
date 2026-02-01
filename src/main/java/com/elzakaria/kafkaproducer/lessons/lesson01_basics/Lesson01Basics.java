package com.elzakaria.kafkaproducer.lessons.lesson01_basics;

import com.elzakaria.kafkaproducer.lessons.Lesson;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Lesson 01: Kafka Producer Basics
 *
 * Learn the three fundamental ways to send messages:
 * 1. Fire-and-forget (async, no waiting)
 * 2. Synchronous send (blocking wait)
 * 3. Asynchronous send with callbacks
 */
@Component
public class Lesson01Basics implements Lesson {

    private static final String TOPIC = "lesson01-basics";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public Lesson01Basics(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public int getLessonNumber() {
        return 1;
    }

    @Override
    public String getTitle() {
        return "Producer Basics - Send Patterns";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers the three fundamental ways to send messages:

            1. FIRE-AND-FORGET: Send without waiting for acknowledgment
               - Fastest but no delivery guarantee
               - Use for: metrics, logs where occasional loss is acceptable

            2. SYNCHRONOUS: Block until broker confirms receipt
               - Slowest but highest reliability
               - Use for: critical transactions, audit logs

            3. ASYNCHRONOUS WITH CALLBACK: Non-blocking with result handling
               - Best balance of performance and reliability
               - Use for: most production scenarios
            """;
    }

    @Override
    public void run() throws Exception {
        // 1. Fire-and-forget
        demonstrateFireAndForget();

        // 2. Synchronous send
        demonstrateSynchronousSend();

        // 3. Asynchronous with callback
        demonstrateAsyncWithCallback();

        tip("In production, async with callbacks is usually the best choice.");
        tip("Check messages in Kafka UI at http://localhost:8080 -> Topics -> " + TOPIC);
    }

    private void demonstrateFireAndForget() {
        explain("1. FIRE-AND-FORGET: Sending without waiting for response");

        step("Sending message without waiting for acknowledgment...");

        // Simple send - we don't wait for the result
        kafkaTemplate.send(TOPIC, "fire-forget-key", "Hello, Kafka! (fire-and-forget)");

        result("Message sent (but we don't know if it succeeded)");
        result("Use case: High-throughput scenarios where occasional message loss is acceptable");
    }

    private void demonstrateSynchronousSend() throws Exception {
        explain("2. SYNCHRONOUS SEND: Blocking until broker confirms");

        step("Sending message and waiting for acknowledgment...");

        // Get the future and block on it
        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(TOPIC, "sync-key", "Hello, Kafka! (synchronous)");

        // Block and wait for the result
        SendResult<String, String> result = future.get();  // <----- WILL throw here if exception if ever.
        RecordMetadata metadata = result.getRecordMetadata();

        // WHEN ACKS is at its lowest you will have a hollow RecordMetadata object
        success("Message confirmed by broker!");
        result("Topic: " + metadata.topic());
        result("Partition: " + metadata.partition());
        result("Offset: " + metadata.offset());
        result("Timestamp: " + metadata.timestamp());

        step("Demonstrating synchronous send with explicit key for partition routing...");

        SendResult<String, String> result2 = kafkaTemplate.send(TOPIC, "user-123", "Order placed for user-123").get();
        result("Same key 'user-123' will always go to partition: " + result2.getRecordMetadata().partition());
    }

    private void demonstrateAsyncWithCallback() throws Exception {
        explain("3. ASYNC WITH CALLBACK: Non-blocking with result handling");

        step("Sending message with success/failure callbacks...");

        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(TOPIC, "async-key", "Hello, Kafka! (async with callback)");

        future.whenComplete((sendResult, exception) -> {
            if (exception == null) {
                RecordMetadata metadata = sendResult.getRecordMetadata();
                success("Async callback - Message delivered to partition " +
                        metadata.partition() + " at offset " + metadata.offset());
            } else {
                error("Async callback - Failed to deliver message: " + exception.getMessage());
            }
        });

        result("Code continues executing while message is being sent...");

        // Send multiple messages asynchronously
        step("Sending batch of messages asynchronously...");

        for (int i = 1; i <= 5; i++) {
            final int messageNum = i;
            kafkaTemplate.send(TOPIC, "batch-key-" + i, "Batch message #" + i)
                    .whenComplete((res, ex) -> {
                        if (ex == null) {
                            result("Message #" + messageNum + " delivered to partition " +
                                    res.getRecordMetadata().partition());
                        }
                    });
        }

        // Wait a bit for async sends to complete
        waitFor(2, "allowing async messages to be delivered");
    }
}
