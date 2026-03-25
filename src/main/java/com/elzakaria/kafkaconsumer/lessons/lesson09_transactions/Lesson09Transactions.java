package com.elzakaria.kafkaconsumer.lessons.lesson09_transactions;

import com.elzakaria.kafkaconsumer.ConsumerUtils;
import com.elzakaria.kafkaconsumer.lessons.ConsumerLesson;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Lesson 09: Transactions & Exactly-Once Semantics (EOS)
 *
 * Implement bulletproof exactly-once processing.
 */
@Component
public class Lesson09Transactions implements ConsumerLesson {

    private static final String TOPIC = "lesson09-transactions";
    private static final String GROUP_ID = "lesson09-group";

    @Override
    public int getLessonNumber() {
        return 9;
    }

    @Override
    public String getTitle() {
        return "Transactions & Exactly-Once Semantics";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers Kafka transactions and EOS:

            1. SEMANTICS GUARANTEES:
               - At-most-once: Message may be lost
               - At-least-once: Message may be duplicated
               - Exactly-once: Each message processed once

            2. KAFKA TRANSACTIONS:
               - Atomic reads and writes across partitions
               - Producer writes to multiple partitions atomically
               - Consumer reads only committed messages

            3. ISOLATION LEVELS:
               - read_uncommitted: See all messages (default)
               - read_committed: See only committed messages
               - Tradeoff: consistency vs availability

            4. EOS WITH EXTERNAL SYSTEM:
               - Goal: Exactly-once write to external database
               - Challenge: Kafka offset and DB in different systems
               - Solution 1: Idempotent database writes (UPSERT)
               - Solution 2: Dual-write with deduplication

            5. TRANSACTIONAL GUARANTEES:
               - All-or-nothing: Either all messages processed or none
               - No partial updates: DB consistency
               - Recovery: Replay from offset

            6. PERFORMANCE IMPLICATIONS:
               - Transactional producers are slower
               - Transactional consumers must wait for commit markers
               - Network and synchronous flush overhead
               - But worth it for critical data

            7. COMMON PATTERNS:
               - Database transaction with offset update
               - Dual-write with unique constraint
               - External offset store (Redis, Postgres)
            """;
    }

    @Override
    public void run() throws Exception {
        ConsumerUtils.createTopics(1, TOPIC);

        produceMessages();

        explainSemantics();
        demonstrateIsolationLevels();
        demonstrateIdempotentProcessing();
        demonstrateExactlyOnceFlow();

        tip("Use Exactly-Once Semantics for financial transactions");
        tip("For non-critical data, At-Least-Once is usually fine");
        tip("Idempotency is the key to EOS");
        warning("EOS has performance cost - use only when necessary!");
    }

    private void produceMessages() throws Exception {
        explain("Producing messages for EOS demo");

        var producerProps = new HashMap<String, Object>();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps)) {
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "tx-1", "TXN:debit:100"));
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "tx-2", "TXN:credit:50"));
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "tx-3", "TXN:debit:75"));
            producer.flush();
            result("Produced 3 transaction messages");
        }
    }

    private void explainSemantics() {
        explain("1. DELIVERY SEMANTICS: Guarantees comparison");

        System.out.println("""
            AT-MOST-ONCE (Fastest, risky):
            ┌────────────────────────────────────────┐
            │ Produces:  Async, no ack                │
            │ Commits:   Immediately after processing │
            │ Crash:     Message lost                 │
            │ Risk:      Data loss                    │
            │ Use case:  Metrics, logs                │
            └────────────────────────────────────────┘

            AT-LEAST-ONCE (Balanced):
            ┌────────────────────────────────────────┐
            │ Produces:  Sync ack before returning    │
            │ Commits:   After processing            │
            │ Crash:     Message reprocessed         │
            │ Risk:      Duplication                 │
            │ Use case:  Most business events       │
            └────────────────────────────────────────┘

            EXACTLY-ONCE (Slowest, safest):
            ┌────────────────────────────────────────┐
            │ Produces:  Transactional writes         │
            │ Commits:   Atomically with offset      │
            │ Crash:     Message processed once      │
            │ Risk:      None (if idempotent)        │
            │ Use case:  Financial transactions      │
            └────────────────────────────────────────┘
            """);
    }

    private void demonstrateIsolationLevels() throws Exception {
        explain("2. ISOLATION LEVELS: read_committed vs read_uncommitted");

        System.out.println("""
            read_uncommitted (default):
            ├─ Consumer sees all messages immediately
            ├─ Fast (no waiting for confirmations)
            ├─ May see aborted transactional messages
            └─ Use: Non-critical data

            read_committed:
            ├─ Consumer sees only confirmed messages
            ├─ Slower (must wait for commit markers)
            ├─ Never sees aborted transactional messages
            └─ Use: Critical financial data

            How it works:
            1. Producer writes messages with transaction ID
            2. Other consumers see "uncommitted" marker
            3. Producer either commits or aborts
            4. read_committed consumers only see commits
            5. read_uncommitted consumers see all
            """);

        step("Testing isolation levels...");

        Map<String, Object> propsUncommitted = createConsumerProps(GROUP_ID + "-u", "read_uncommitted");
        Map<String, Object> propsCommitted = createConsumerProps(GROUP_ID + "-c", "read_committed");

        result("Created two consumers with different isolation levels");
        result("  Consumer 1: isolation.level = read_uncommitted");
        result("  Consumer 2: isolation.level = read_committed");

        tip("In production, use read_committed for financial transactions");
    }

    private void demonstrateIdempotentProcessing() throws Exception {
        explain("3. IDEMPOTENT PROCESSING: Safe deduplication");

        System.out.println("""
            Idempotent Database Operations:

            WRONG (non-idempotent):
            INSERT INTO accounts (user_id, balance) VALUES (123, 100);
            Result: If reprocessed, duplicate account created!

            CORRECT (idempotent):
            INSERT INTO accounts (user_id, balance) VALUES (123, 100)
            ON DUPLICATE KEY UPDATE balance = 100;
            Result: Same state if executed 1 or 100 times

            Pattern: Always use UPSERT, not INSERT

            Implementation:
            1. Extract unique message_id from Kafka message
            2. Use as unique constraint in database
            3. Use UPSERT (INSERT...ON DUPLICATE KEY UPDATE)
            4. If reprocessed, same result (idempotent)

            Database schema:
            CREATE TABLE messages (
                message_id VARCHAR(100) PRIMARY KEY,
                user_id INT,
                amount DECIMAL,
                processed_at TIMESTAMP
            );
            """);

        step("Simulating idempotent write...");

        String messageId = "msg-12345";
        String userId = "user-789";
        double amount = 100.0;

        result("Processing message: " + messageId);
        result("  INSERT INTO transactions");
        result("    (message_id, user_id, amount)");
        result("  VALUES ('" + messageId + "', '" + userId + "', " + amount + ")");
        result("  ON DUPLICATE KEY UPDATE amount = " + amount);

        // Simulate reprocessing
        result("Reprocessing same message (crash scenario)...");
        result("  Same SQL executed again");
        result("  ON DUPLICATE KEY UPDATE triggers");
        result("  Same result = idempotent!");

        success("Idempotent processing achieved!");

        tip("Message ID = Kafka offset or unique transaction ID");
        tip("Unique constraint prevents duplicates");
        tip("UPSERT pattern prevents data corruption");
    }

    private void demonstrateExactlyOnceFlow() throws Exception {
        explain("4. EXACTLY-ONCE FLOW: End-to-end implementation");

        step("E2E exactly-once processing flow:");

        System.out.println("""
            1. CONSUMER receives message
               ├─ Isolation level = read_committed
               └─ Only sees confirmed messages

            2. VALIDATE message
               ├─ Deserialize
               ├─ Check schema
               └─ Verify business rules

            3. PROCESS message
               └─ Compute result

            4. UPDATE DATABASE (atomically)
               ├─ Check dedup table (message_id)
               ├─ If new: INSERT with message_id
               ├─ If duplicate: Skip (already processed)
               └─ Transaction commits

            5. COMMIT OFFSET
               └─ consumer.commitSync()
               
            6. IF CRASH between 4-5
               ├─ Consumer restarts
               ├─ Reads from last committed offset
               ├─ Reprocesses message
               ├─ Dedup table prevents duplicate
               └─ Message processed exactly once!

            Key insight:
            ├─ Dedup table = idempotency
            ├─ Database transaction = atomicity
            ├─ Offset commit = progress checkpoint
            └─ Combination = exactly-once
            """);

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-eos", "read_committed");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            result("Consumer ready for EOS processing");
            result("Isolation level: read_committed");
            result("Auto-commit: disabled");

            tip("Always use commitSync() for EOS");
            tip("Never use auto-commit with EOS");
            tip("Monitor dedup table size");
        }
    }

    private Map<String, Object> createConsumerProps(String groupId, String isolationLevel) {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("enable.auto.commit", false);
        props.put("auto.offset.reset", "earliest");
        props.put("isolation.level", isolationLevel);
        return props;
    }
}

