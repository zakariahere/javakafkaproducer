package com.elzakaria.kafkaconsumer.lessons.lesson03_offsets;

import com.elzakaria.kafkaconsumer.ConsumerUtils;
import com.elzakaria.kafkaconsumer.lessons.ConsumerLesson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Lesson 03: Offset Management - Commit Strategies
 *
 * Understand offset tracking, commits, and exactly-once semantics.
 */
@Component
public class Lesson03OffsetManagement implements ConsumerLesson {

    private static final String TOPIC = "lesson03-offsets";
    private static final String GROUP_ID = "lesson03-group";

    @Override
    public int getLessonNumber() {
        return 3;
    }

    @Override
    public String getTitle() {
        return "Offset Management - Commit Strategies";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers offset tracking and commit strategies:

            1. OFFSET CONCEPT:
               - Position of the next message to consume in a partition
               - Stored in special __consumer_offsets topic by broker
               - Allows consumer to resume from where it left off after restart

            2. AUTO-COMMIT vs MANUAL COMMIT:
               - Auto-commit: Kafka commits every auto.commit.interval.ms
                 PRO: Simple, less code
                 CON: Risk of message loss or duplicate processing
               - Manual commit: Application decides when to commit
                 PRO: Full control, exactly-once possible
                 CON: More code, must handle carefully

            3. COMMIT MODES:
               - commitSync(): Blocking, ensures offset is committed before returning
               - commitAsync(): Non-blocking, offset committed in background
               - Both can accept specific offsets or use current position

            4. EXACTLY-ONCE SEMANTICS:
               - Commit offset AFTER processing message
               - If crash before commit: message reprocessed (acceptable)
               - If crash after commit: message skipped (not reprocessed)
               - Goal: Each message processed exactly once

            5. OFFSET RESET BEHAVIOR (auto.offset.reset):
               - earliest: Start from beginning
               - latest: Start from end (skip old messages)
               - none: Throw error if no offset found

            6. DANGEROUS PATTERNS:
               - Committing before processing (lose messages)
               - Using auto-commit with long processing (duplicate processing)
               - Ignoring commit errors
            """;
    }

    @Override
    public void run() throws Exception {
        ConsumerUtils.createTopics(2, TOPIC);

        produceMessages();

        demonstrateAutoCommit();
        demonstrateManualCommitSync();
        demonstrateManualCommitAsync();
        demonstrateSeekingOffsets();
        demonstrateOffsetReset();
        demonstrateConsumerLag();

        tip("Use manual commits with commitSync() for reliability");
        tip("Monitor consumer lag - indicates processing health");
        warning("NEVER commit before processing - data loss risk!");
        warning("NEVER use auto-commit with long processing - duplication risk!");
    }

    private void produceMessages() throws Exception {
        explain("Producing test messages");

        var producerProps = new HashMap<String, Object>();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps)) {
            for (int i = 1; i <= 20; i++) {
                producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                        TOPIC, "key-" + i, "Message #" + i
                ));
            }
            producer.flush();
            result("Produced 20 messages");
        }
    }

    private void demonstrateAutoCommit() throws Exception {
        explain("1. AUTO-COMMIT: Kafka automatically commits offsets");

        step("Creating consumer with enable.auto.commit=true...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-auto");
        props.put("enable.auto.commit", true);
        props.put("auto.commit.interval.ms", 1000);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            result("Polling 5 times with auto-commit...");
            int messageCount = 0;

            for (int poll = 0; poll < 6; poll++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    messageCount++;
                    System.out.println("    Message: offset=" + record.offset() + ", value=" + record.value());
                }

                if (!records.isEmpty()) {
                    result("Poll #" + (poll + 1) + ": consumed " + records.count() + " messages");
                    // Auto-commit happens periodically in background
                    waitFor(1, "allowing auto-commit to happen");
                }
            }

            success("Consumed " + messageCount + " messages with auto-commit");

            tip("Auto-commit is convenient but risky");
            warning("If consumer crashes between auto-commit cycles, messages are reprocessed!");
            warning("Use auto-commit only for non-critical data");
        }
    }

    private void demonstrateManualCommitSync() throws Exception {
        explain("2. MANUAL COMMIT SYNC: Blocking commit after processing");

        step("Creating consumer with manual commit...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-manual-sync");
        props.put("enable.auto.commit", false);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            result("Polling and committing each batch synchronously...");
            int totalConsumed = 0;

            for (int poll = 0; poll < 6; poll++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                if (!records.isEmpty()) {
                    result("Poll #" + (poll + 1) + ": received " + records.count() + " messages");

                    // Application processes messages
                    for (ConsumerRecord<String, String> record : records) {
                        simulateProcessing(record);
                        totalConsumed++;
                    }

                    // Commit only after processing
                    step("Committing offsets synchronously...");
                    consumer.commitSync();
                    result("Offsets committed - safe to fail now");
                }
            }

            success("Consumed and committed " + totalConsumed + " messages");

            tip("commitSync() blocks until broker confirms offset is stored");
            tip("Safe but slower - use for critical data");
            tip("If crash after commitSync(), no reprocessing needed");
        }
    }

    private void demonstrateManualCommitAsync() throws Exception {
        explain("3. MANUAL COMMIT ASYNC: Non-blocking commit with callback");

        step("Creating consumer with async commits...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-manual-async");
        props.put("enable.auto.commit", false);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            result("Polling and committing asynchronously...");
            int totalConsumed = 0;

            for (int poll = 0; poll < 6; poll++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                if (!records.isEmpty()) {
                    result("Poll #" + (poll + 1) + ": received " + records.count() + " messages");

                    for (ConsumerRecord<String, String> record : records) {
                        simulateProcessing(record);
                        totalConsumed++;
                    }

                    step("Committing asynchronously with callback...");
                    consumer.commitAsync((offsets, exception) -> {
                        if (exception == null) {
                            result("Async commit succeeded for offsets: " + offsets);
                        } else {
                            error("Async commit failed: " + exception.getMessage());
                        }
                    });

                    result("Code continues immediately (non-blocking)");
                }
            }

            // Final sync commit to ensure all are committed before shutdown
            consumer.commitSync();
            success("Final sync commit done");

            tip("commitAsync() returns immediately - faster");
            tip("Use callback to handle failures");
            tip("End with commitSync() to ensure final commits");
        }
    }

    private void demonstrateSeekingOffsets() throws Exception {
        explain("4. SEEKING: Moving to specific offsets");

        step("Seeking to specific offset in partition...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-seek");
        props.put("enable.auto.commit", false);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(Collections.singletonList(new TopicPartition(TOPIC, 0)));

            // Seek to offset 5
            step("Seeking to offset 5 in partition 0...");
            consumer.seek(new TopicPartition(TOPIC, 0), 5);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            result("Read " + records.count() + " messages starting from offset 5:");

            records.forEach(r -> System.out.println("    Offset " + r.offset() + ": " + r.value()));

            // Seek to beginning
            step("Seeking to beginning of partition...");
            consumer.seekToBeginning(Collections.singletonList(new TopicPartition(TOPIC, 0)));

            records = consumer.poll(Duration.ofSeconds(1));
            result("First few messages from beginning:");

            int count = 0;
            for (ConsumerRecord<String, String> r : records) {
                if (count++ < 3) {
                    System.out.println("    Offset " + r.offset() + ": " + r.value());
                }
            }

            // Seek to end
            step("Seeking to end of partition...");
            consumer.seekToEnd(Collections.singletonList(new TopicPartition(TOPIC, 0)));

            records = consumer.poll(Duration.ofMillis(500));
            result("Messages from end: " + records.count() + " (should be 0)");

            tip("seek() allows replay of messages");
            tip("Use seekToBeginning() for complete replay");
            tip("Use seekToEnd() to skip to latest");
        }
    }

    private void demonstrateOffsetReset() throws Exception {
        explain("5. OFFSET RESET: Behavior when no offset found");

        System.out.println("""
            auto.offset.reset determines what happens when:
            - Consumer group has no committed offset
            - Consumer is new to the topic
            - Offset was deleted from broker

            ┌─────────────────────────────────────────┐
            │ earliest: Start from beginning          │
            │ → Reprocess all messages                │
            │ → Good for: Backfill, replay            │
            │                                         │
            │ latest: Start from newest message       │
            │ → Skip old messages                     │
            │ → Good for: Starting consumer on live   │
            │                                         │
            │ none: Throw NoOffsetForPartitionException
            │ → Good for: Debugging, explicit control │
            └─────────────────────────────────────────┘
            """);

        result("Testing auto.offset.reset=earliest behavior:");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-reset-" + System.currentTimeMillis());
        props.put("enable.auto.commit", false);
        props.put("auto.offset.reset", "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            result("With auto.offset.reset=earliest: read " + records.count() + " messages");

            if (!records.isEmpty()) {
                // Get first record from the topic
                ConsumerRecord<String, String> first = null;
                for (ConsumerRecord<String, String> record : records.records(TOPIC)) {
                    first = record;
                    break;
                }
                if (first != null) {
                    result("First offset: " + first.offset());
                    success("Started from beginning!");
                }
            }
        }
    }

    private void demonstrateConsumerLag() throws Exception {
        explain("6. CONSUMER LAG: Monitoring processing speed");

        step("Calculating consumer lag...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-lag");
        props.put("enable.auto.commit", false);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            // Get some records
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            if (!records.isEmpty()) {
                // Commit what we have so far
                consumer.commitSync();

                // Get the lag for each partition
                result("Consumer lag per partition:");

                for (TopicPartition tp : consumer.assignment()) {
                    long committed = consumer.committed(tp).offset();
                    long endOffsets = consumer.endOffsets(Collections.singletonList(tp)).get(tp);

                    long lag = endOffsets - committed;

                    System.out.println("    Partition " + tp.partition() +
                            ": committed=" + committed +
                            ", endOffset=" + endOffsets +
                            ", lag=" + lag);
                }

                result("Total messages behind: ~" + ((long) records.count() * 2));
            }

            tip("Consumer lag = endOffset - committedOffset");
            tip("Monitor lag in production:");
            tip("  - Lag increasing = falling behind");
            tip("  - Lag constant = processing speed matches producer");
            tip("  - Lag zero = fully caught up");
        }
    }

    private void simulateProcessing(ConsumerRecord<String, String> record) {
        // Simulate some processing
        try {
            Thread.sleep(50);
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
        props.put("auto.offset.reset", "earliest");
        return props;
    }
}

