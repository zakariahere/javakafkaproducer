package com.elzakaria.kafkaproducer.lessons.lesson04_callbacks;

import com.elzakaria.kafkaproducer.lessons.Lesson;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lesson 04: Callbacks and Futures
 *
 * Learn how to handle producer results:
 * 1. CompletableFuture basics
 * 2. Success and failure callbacks
 * 3. Chaining operations
 * 4. Batch completion tracking
 */
@Component
public class Lesson04Callbacks implements Lesson {

    private static final String TOPIC = "lesson04-callbacks";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public Lesson04Callbacks(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public int getLessonNumber() {
        return 4;
    }

    @Override
    public String getTitle() {
        return "Callbacks & Futures - Handling Results";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers handling send results with CompletableFuture:

            1. COMPLETABLEFUTURE: Modern async programming
               - .get() - blocking wait
               - .whenComplete() - callback on completion
               - .thenApply() - transform result
               - .exceptionally() - handle errors

            2. RECORDMETADATA: Information about sent message
               - topic, partition, offset
               - timestamp, serialized sizes

            3. BATCH TRACKING: Coordinating multiple sends
               - CountDownLatch for synchronization
               - Collecting all results
            """;
    }

    @Override
    public void run() throws Exception {
        demonstrateBasicFuture();
        demonstrateWhenComplete();
        demonstrateChainingOperations();
        demonstrateBatchTracking();

        tip("Always handle both success and failure in production code.");
        tip("Use whenComplete() for side effects, thenApply() for transformations.");
    }

    private void demonstrateBasicFuture() throws Exception {
        explain("1. COMPLETABLEFUTURE BASICS: Blocking vs non-blocking");

        step("Blocking with .get()...");

        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(TOPIC, "key-1", "Message with blocking get()");

        // Blocking - wait for result
        SendResult<String, String> sendResult = future.get();
        RecordMetadata metadata = sendResult.getRecordMetadata();

        success("Message sent successfully!");
        printMetadata(metadata);

        step("Blocking with timeout...");

        future = kafkaTemplate.send(TOPIC, "key-2", "Message with timeout");

        try {
            sendResult = future.get(5, TimeUnit.SECONDS);
            success("Message sent within timeout");
        } catch (Exception e) {
            error("Timeout or error: " + e.getMessage());
        }

        step("Non-blocking check with isDone()...");

        future = kafkaTemplate.send(TOPIC, "key-3", "Non-blocking check message");

        // Poll until done (not recommended, just for demonstration)
        while (!future.isDone()) {
            result("Still sending...");
            Thread.sleep(10);
        }

        success("Message confirmed: " + future.get().getRecordMetadata().offset());
    }

    private void demonstrateWhenComplete() throws Exception {
        explain("2. WHENCOMPLETE: Callback on success or failure");

        step("Using whenComplete() for callback...");

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        for (int i = 1; i <= 5; i++) {
            final int msgNum = i;

            kafkaTemplate.send(TOPIC, "callback-" + i, "Callback message #" + i)
                    .whenComplete((sendResult, exception) -> {
                        if (exception == null) {
                            successCount.incrementAndGet();
                            RecordMetadata m = sendResult.getRecordMetadata();
                            System.out.println("    [Callback] #" + msgNum + " -> " +
                                    "partition=" + m.partition() + ", offset=" + m.offset());
                        } else {
                            failureCount.incrementAndGet();
                            System.out.println("    [Callback] #" + msgNum + " FAILED: " +
                                    exception.getMessage());
                        }
                    });
        }

        waitFor(2, "callbacks to execute");

        result("Success: " + successCount.get() + ", Failures: " + failureCount.get());

        step("Using separate success/failure handlers...");

        kafkaTemplate.send(TOPIC, "handler-key", "Message with separate handlers")
                .thenAccept(sendResult -> {
                    // Only called on success
                    success("thenAccept: Message delivered to offset " +
                            sendResult.getRecordMetadata().offset());
                })
                .exceptionally(ex -> {
                    // Only called on failure
                    error("exceptionally: " + ex.getMessage());
                    return null;
                });

        waitFor(1, "handlers to execute");
    }

    private void demonstrateChainingOperations() throws Exception {
        explain("3. CHAINING OPERATIONS: Transform and combine results");

        step("Transforming result with thenApply()...");

        String resultString = kafkaTemplate.send(TOPIC, "chain-key", "Chained message")
                .thenApply(sendResult -> {
                    RecordMetadata m = sendResult.getRecordMetadata();
                    return "Stored at " + m.topic() + ":" + m.partition() + ":" + m.offset();
                })
                .get();

        result("Transformed result: " + resultString);

        step("Chaining multiple operations...");

        kafkaTemplate.send(TOPIC, "multi-chain", "Multi-chain message")
                .thenApply(result -> {
                    System.out.println("    Step 1: Extract metadata");
                    return result.getRecordMetadata();
                })
                .thenApply(metadata -> {
                    System.out.println("    Step 2: Build confirmation");
                    return "Confirmed: offset=" + metadata.offset();
                })
                .thenAccept(confirmation -> {
                    System.out.println("    Step 3: " + confirmation);
                })
                .get();

        success("Chain completed");

        step("Combining multiple futures...");

        CompletableFuture<SendResult<String, String>> future1 =
                kafkaTemplate.send(TOPIC, "combine-1", "First message");
        CompletableFuture<SendResult<String, String>> future2 =
                kafkaTemplate.send(TOPIC, "combine-2", "Second message");

        // Wait for both
        CompletableFuture.allOf(future1, future2).get();

        result("Both messages sent: offset1=" + future1.get().getRecordMetadata().offset() +
                ", offset2=" + future2.get().getRecordMetadata().offset());
    }

    private void demonstrateBatchTracking() throws Exception {
        explain("4. BATCH TRACKING: Coordinating multiple sends");

        step("Using CountDownLatch for batch completion...");

        int batchSize = 10;
        CountDownLatch latch = new CountDownLatch(batchSize);
        List<Long> offsets = new ArrayList<>();
        AtomicInteger failures = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < batchSize; i++) {
            kafkaTemplate.send(TOPIC, "batch-" + i, "Batch message #" + i)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            synchronized (offsets) {
                                offsets.add(result.getRecordMetadata().offset());
                            }
                        } else {
                            failures.incrementAndGet();
                        }
                        latch.countDown();
                    });
        }

        // Wait for all messages
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - startTime;

        if (completed) {
            success("Batch of " + batchSize + " messages sent in " + duration + "ms");
            result("Successful: " + offsets.size() + ", Failed: " + failures.get());
            result("Offsets: " + offsets.subList(0, Math.min(5, offsets.size())) + "...");
        } else {
            error("Batch timed out!");
        }

        step("Collecting all results with allOf()...");

        List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            futures.add(kafkaTemplate.send(TOPIC, "collect-" + i, "Collect message #" + i));
        }

        // Wait for all and collect results
        CompletableFuture<Void> allDone = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
        );

        allDone.get();

        result("All futures completed:");
        for (int i = 0; i < futures.size(); i++) {
            RecordMetadata m = futures.get(i).get().getRecordMetadata();
            result("  Message " + i + ": partition=" + m.partition() + ", offset=" + m.offset());
        }
    }

    private void printMetadata(RecordMetadata metadata) {
        result("RecordMetadata:");
        result("  topic: " + metadata.topic());
        result("  partition: " + metadata.partition());
        result("  offset: " + metadata.offset());
        result("  timestamp: " + metadata.timestamp());
        result("  serializedKeySize: " + metadata.serializedKeySize());
        result("  serializedValueSize: " + metadata.serializedValueSize());
    }
}
