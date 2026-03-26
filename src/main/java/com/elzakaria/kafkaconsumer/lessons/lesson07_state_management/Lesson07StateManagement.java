package com.elzakaria.kafkaconsumer.lessons.lesson07_state_management;

import com.elzakaria.kafkaconsumer.ConsumerUtils;
import com.elzakaria.kafkaconsumer.lessons.ConsumerLesson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Lesson 07: Offset & State Management
 *
 * Manage application state and ensure consistency.
 */
@Component
public class Lesson07StateManagement implements ConsumerLesson {

    private static final String TOPIC = "lesson07-state";
    private static final String GROUP_ID = "lesson07-group";

    @Override
    public int getLessonNumber() {
        return 7;
    }

    @Override
    public String getTitle() {
        return "State & Offset Management - Consistency Patterns";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers state management and consistency:

            1. STATELESS vs STATEFUL CONSUMERS:
               - Stateless: Process message independently, no memory
                 PRO: Simple, scalable, fault tolerant
                 CON: Can't do aggregations or correlations
               - Stateful: Maintain state across messages
                 PRO: Complex processing, analytics
                 CON: State management complexity, recovery

            2. EXACTLY-ONCE SEMANTICS:
               - Goal: Each message processed exactly once
               - Challenge: Consumer crashes between processing and commit
               - Solution: Atomic commit of offset + application state

            3. CONSISTENCY GUARANTEES:
               - At-most-once: Message lost if consumer crashes before commit
               - At-least-once: Message reprocessed if consumer crashes
               - Exactly-once: Requires atomic offset + state update

            4. IDEMPOTENT PROCESSING:
               - Process same message multiple times = same result
               - Use unique message ID tracking
               - Upsert in database (not insert)

            5. CHECKPOINT PATTERN:
               - Periodically save processing position
               - In external DB, not just memory
               - Allows recovery without reprocessing

            6. STATE RECOVERY:
               - After crash, resume from last committed offset
               - Rebuild in-memory state from changelog topic
               - Or load from snapshot in database
            """;
    }

    @Override
    public void run() throws Exception {
        ConsumerUtils.createTopics(2, TOPIC);

        produceMessages();

        demonstrateStatelessConsumer();
        demonstrateStatefulConsumer();
        demonstrateIdempotentProcessing();
        demonstrateCheckpointPattern();

        tip("Prefer stateless when possible - simpler and more scalable");
        tip("Use idempotent operations (upsert, not insert)");
        tip("Always save state alongside offset commits");
        warning("Losing offset + state = message reprocessing or data loss!");
    }

    private void produceMessages() throws Exception {
        explain("Producing test messages");

        var producerProps = new HashMap<String, Object>();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps)) {
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "user-1", "ORDER:100"));
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "user-1", "PAYMENT:100"));
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "user-2", "ORDER:50"));
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "user-2", "REFUND:25"));
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "user-1", "DELIVERY:complete"));
            producer.flush();
            result("Produced 5 messages");
        }
    }

    private void demonstrateStatelessConsumer() throws Exception {
        explain("1. STATELESS CONSUMER: Process each message independently");

        step("Consuming and processing without state...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-stateless");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            for (ConsumerRecord<String, String> record : records) {
                // Process each message independently
                String[] parts = record.value().split(":");
                String action = parts[0];
                String amount = parts.length > 1 ? parts[1] : "0";

                System.out.println("    User " + record.key() + ": " + action + " " + amount);
            }

            consumer.commitSync();
            success("Stateless processing completed");

            tip("Each message is independent");
            tip("No need to track state between messages");
            tip("Easy to parallelize across partitions");
        }
    }

    private void demonstrateStatefulConsumer() throws Exception {
        explain("2. STATEFUL CONSUMER: Maintain state across messages");

        step("Consuming and aggregating user balances...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-stateful");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            // In-memory state (in production, use database)
            Map<String, Integer> userBalances = new HashMap<>();

            int processedCount = 0;
            for (int poll = 0; poll < 11; poll++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    String user = record.key();
                    String[] parts = record.value().split(":");
                    String action = parts[0];
                    int amount = parts.length > 1 && !Objects.equals(parts[1], "complete") ? Integer.parseInt(parts[1]) : 0;

                    // Update state
                    int currentBalance = userBalances.getOrDefault(user, 0);
                    int newBalance = currentBalance;

                    if (action.equals("ORDER")) {
                        newBalance = currentBalance + amount;
                    } else if (action.equals("REFUND")) {
                        newBalance = currentBalance - amount;
                    }

                    userBalances.put(user, newBalance);
                    processedCount++;
                }

                // Commit offset only after state is safe
                if (!records.isEmpty()) {
                    consumer.commitSync();
                    result("Committed after processing batch");
                }
            }

            System.out.println("\n  Final balances:");
            userBalances.forEach((user, balance) ->
                    System.out.println("    " + user + ": " + balance));

            success("Processed " + processedCount + " messages with state");

            warning("State lost if consumer crashes before commit!");
            warning("In production, save state to database");
        }
    }

    private void demonstrateIdempotentProcessing() throws Exception {
        explain("3. IDEMPOTENT PROCESSING: Safe message reprocessing");

        step("Idempotent operation example:");

        System.out.println("""
            IDEMPOTENT (safe to reprocess):
            UPDATE users SET balance = 100 WHERE user_id = 'user-1';
            → Same result if executed 1 time or 10 times

            NON-IDEMPOTENT (dangerous):
            UPDATE users SET balance = balance + 100 WHERE user_id = 'user-1';
            → Reprocessing adds 100 multiple times!

            Solution: Use unique message IDs:
            INSERT INTO dedup_log (message_id, user_id) VALUES (msg-123, user-1);
            IF unique constraint passes:
                UPDATE users SET balance = 100 WHERE user_id = 'user-1';
            """);

        result("Pattern: Message ID deduplication");
        result("1. Extract message_id from record");
        result("2. Check if already processed (database or cache)");
        result("3. If new, process and insert dedup record");
        result("4. If duplicate, skip processing");

        success("Idempotent processing enables safe message replay");

        tip("Use UPSERT for state: INSERT...UPDATE instead of INSERT");
        tip("Maintain dedup table with message IDs");
        tip("Set message ID as unique constraint");
    }

    private void demonstrateCheckpointPattern() throws Exception {
        explain("4. CHECKPOINT PATTERN: Periodic state snapshots");

        System.out.println("""
            Checkpoint Strategy:

            Every N messages or T seconds:
            1. Save application state to external DB
            2. Save current offset
            3. Both updates atomic if possible

            On recovery:
            1. Load last checkpoint from DB
            2. Consumer resumes from saved offset
            3. Skip already-processed messages

            Benefits:
            - Faster recovery (don't reprocess everything)
            - External durability (not just consumer memory)
            - Ability to detect gaps/inconsistencies
            """);

        result("Checkpoint example:");
        System.out.println("""
            CREATE TABLE checkpoints (
                consumer_group VARCHAR(100),
                topic VARCHAR(100),
                partition INT,
                offset BIGINT,
                state_json TEXT,
                updated_at TIMESTAMP,
                PRIMARY KEY (consumer_group, topic, partition)
            );

            Every 1000 messages:
            INSERT INTO checkpoints (...) VALUES (...)
            ON DUPLICATE KEY UPDATE offset=..., state_json=...;
            """);

        result("Checkpoint frequency:");
        result("  - Too frequent: Overhead");
        result("  - Too rare: Long recovery time");
        result("  - Recommended: Every 1000-10000 messages or 60s");

        tip("Checkpoints are not required, but recommended for large state");
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

