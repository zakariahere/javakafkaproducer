package com.elzakaria.kafkaconsumer.lessons.lesson07_concurrency;

import com.elzakaria.kafkaconsumer.ConsumerUtils;
import com.elzakaria.kafkaconsumer.lessons.Lesson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Lesson 07: Concurrency & Threading
 *
 * Master thread safety and concurrent message processing.
 */
@Component
public class Lesson07Concurrency implements Lesson {

    private static final String TOPIC = "lesson07-concurrency";
    private static final String GROUP_ID = "lesson07-group";

    @Override
    public int getLessonNumber() {
        return 7;
    }

    @Override
    public String getTitle() {
        return "Concurrency & Threading - Async Processing";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers threading models and concurrent processing:

            1. SINGLE-THREADED MODEL:
               - KafkaConsumer is NOT thread-safe
               - Must be used from single thread only
               - All poll() calls from same thread
               - All offset commits from same thread

            2. MULTI-THREADED PROCESSING:
               - Poll in single thread (polling thread)
               - Pass records to thread pool for processing
               - Wait for batch completion before next poll

            3. THREAD SAFETY PATTERNS:
               - Synchronization: lock-based (slow)
               - Message passing: queue between threads (preferred)
               - ExecutorService: thread pool management

            4. ORDERING GUARANTEES:
               - Single partition: Messages processed in order if single thread
               - Multiple partitions: No ordering across partitions
               - If processing in thread pool, order may be lost

            5. DEADLOCK PREVENTION:
               - Don't poll while processing thread blocks
               - Don't hold consumer lock while waiting
               - Use separate queues for thread communication

            6. GRACEFUL SHUTDOWN:
               - Stop polling
               - Wait for in-flight messages to complete
               - Commit final offset
               - Close consumer

            7. PERFORMANCE CONSIDERATIONS:
               - Thread pool size: CPU cores for CPU-bound, higher for I/O
               - Queue sizes: Limit in-flight messages
               - Thread affinity: Keep threads on same CPU
            """;
    }

    @Override
    public void run() throws Exception {
        ConsumerUtils.createTopics(2, TOPIC);

        produceMessages();

        demonstrateSingleThreadedPolling();
        demonstrateAsyncProcessing();
        demonstrateThreadPool();
        demonstrateGracefulShutdown();

        tip("KafkaConsumer is NOT thread-safe - use from single thread");
        tip("Use thread pool for I/O-bound processing");
        tip("Always wait for in-flight messages before shutdown");
        warning("Don't block polling thread - offsets won't be committed!");
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

    private void demonstrateSingleThreadedPolling() throws Exception {
        explain("1. SINGLE-THREADED: Traditional synchronous processing");

        step("Polling and processing in same thread...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-single");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            long startTime = System.currentTimeMillis();
            int messagesProcessed = 0;

            for (int poll = 0; poll < 6; poll++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    // Process synchronously (blocks)
                    processMessage(record);
                    messagesProcessed++;
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }

            long duration = System.currentTimeMillis() - startTime;

            success("Processed " + messagesProcessed + " messages in " + duration + "ms");
            result("Average time per message: " + (duration / Math.max(messagesProcessed, 1)) + "ms");

            tip("Simple and straightforward");
            tip("Maintains message ordering within partition");
            warning("Slower - can't process while waiting for I/O");
        }
    }

    private void demonstrateAsyncProcessing() throws Exception {
        explain("2. ASYNC PROCESSING: Submit to thread pool");

        step("Polling and submitting to executor...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-async");

        ExecutorService executor = Executors.newFixedThreadPool(4);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            List<Future<Void>> futures = new ArrayList<>();

            long startTime = System.currentTimeMillis();

            for (int poll = 0; poll < 6; poll++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    // Submit to thread pool (non-blocking)
                    Future<Void> future = executor.submit(() -> {
                        processMessage(record);
                        return null;
                    });
                    futures.add(future);
                }
            }

            // Wait for all to complete
            result("Waiting for " + futures.size() + " async tasks...");
            for (Future<Void> future : futures) {
                future.get();
            }

            long duration = System.currentTimeMillis() - startTime;

            consumer.commitSync();
            success("All messages processed in " + duration + "ms");

            tip("Faster because processing can overlap");
            tip("Use for I/O-bound operations (API calls, DB queries)");
            warning("Loses ordering across threads!");
        } finally {
            executor.shutdown();
        }
    }

    private void demonstrateThreadPool() throws Exception {
        explain("3. THREAD POOL SIZING: Tuning for performance");

        System.out.println("""
            Thread Pool Size Recommendations:

            CPU-Bound (compute-heavy):
            ├─ Optimal: # CPU cores
            └─ Reason: More threads = more context switching overhead

            I/O-Bound (network, database):
            ├─ Optimal: # CPU cores × 2-4
            └─ Reason: Threads wait for I/O, others can work

            Queue Size:
            ├─ Too small: Rejections when submitting
            ├─ Too large: High memory, high latency spikes
            └─ Optimal: Max in-flight messages / 2

            Example:
            cores = 4
            CPU-bound: pool = 4
            I/O-bound (DB): pool = 8-16
            Queue: pool × 50 = 400-800
            """);

        ExecutorService executor = new ThreadPoolExecutor(
                4,                           // core threads
                16,                          // max threads
                60, TimeUnit.SECONDS,        // keep-alive time
                new LinkedBlockingQueue<>(800)  // queue
        );

        result("Thread pool configured:");
        result("  Core threads: 4");
        result("  Max threads: 16 (scales for burst load)");
        result("  Queue size: 800");

        executor.shutdown();
        success("Pool sizing explains latency vs throughput tradeoffs");
    }

    private void demonstrateGracefulShutdown() throws Exception {
        explain("4. GRACEFUL SHUTDOWN: Proper cleanup sequence");

        System.out.println("""
            Shutdown Sequence:

            1. STOP POLLING
               └─ consumer.wakeup() or stop poll loop

            2. WAIT FOR IN-FLIGHT
               ├─ executor.shutdown()
               └─ awaitTermination(timeout, unit)

            3. FINAL COMMIT
               ├─ consumer.commitSync() (with timeout)
               └─ Ensures last batch is committed

            4. CLOSE RESOURCES
               ├─ consumer.close()
               └─ executor.shutdownNow()

            Failure mode if skipped:
            ├─ Without step 2: Consumer crashes, messages reprocessed
            ├─ Without step 3: In-flight messages lost
            └─ Without step 4: Resource leaks
            """);

        result("Best practice: Use try-with-resources for consumer");
        result("Use executor.awaitTermination() with timeout");
        result("Add shutdown hooks for graceful termination");

        tip("Timeout graceful shutdown - forcefully kill after delay");
    }

    private void processMessage(ConsumerRecord<String, String> record) {
        try {
            // Simulate I/O operation
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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

