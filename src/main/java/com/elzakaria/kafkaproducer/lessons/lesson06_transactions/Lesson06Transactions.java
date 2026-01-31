package com.elzakaria.kafkaproducer.lessons.lesson06_transactions;

import com.elzakaria.kafkaproducer.lessons.Lesson;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Lesson 06: Transactional Producer
 *
 * Learn exactly-once semantics with Kafka transactions:
 * 1. Transaction basics
 * 2. Atomic multi-message sends
 * 3. Commit and abort
 * 4. Read committed isolation
 */
@Component
public class Lesson06Transactions implements Lesson {

    private static final String TOPIC_ORDERS = "lesson06-orders";
    private static final String TOPIC_INVENTORY = "lesson06-inventory";
    private static final String TOPIC_NOTIFICATIONS = "lesson06-notifications";

    private final Map<String, Object> baseProps;

    public Lesson06Transactions() {
        this.baseProps = new HashMap<>();
        baseProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        baseProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        baseProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    @Override
    public int getLessonNumber() {
        return 6;
    }

    @Override
    public String getTitle() {
        return "Transactions - Exactly-Once Semantics";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers Kafka transactions for exactly-once delivery:

            1. TRANSACTION BASICS:
               - transactional.id: Unique identifier for producer instance
               - All-or-nothing delivery across multiple topics/partitions

            2. TRANSACTION LIFECYCLE:
               - beginTransaction() -> send() -> commitTransaction()
               - abortTransaction() on failure

            3. EXACTLY-ONCE SEMANTICS (EOS):
               - Combines idempotence + transactions
               - Prevents duplicates even with retries
               - Atomic across multiple topics

            4. ISOLATION LEVELS:
               - read_uncommitted: See all messages (default)
               - read_committed: Only see committed transactions

            Use cases: Financial transactions, inventory updates, multi-system coordination
            """;
    }

    @Override
    public void run() throws Exception {
        explainTransactionConcepts();
        demonstrateBasicTransaction();
        demonstrateMultiTopicTransaction();
        demonstrateTransactionAbort();

        tip("Use unique transactional.id per producer instance.");
        tip("Transactions add latency - use only when needed.");
    }

    private void explainTransactionConcepts() {
        explain("1. TRANSACTION CONCEPTS: Understanding exactly-once");

        System.out.println("""

            Why Transactions?
            ┌──────────────────────────────────────────────────────────────┐
            │ Problem: Order processing writes to 3 topics                 │
            │                                                              │
            │   1. orders topic       - Order created                      │
            │   2. inventory topic    - Stock decreased                    │
            │   3. notifications topic - Email notification                │
            │                                                              │
            │ Without transactions:                                        │
            │   - Partial writes possible if failure after step 2          │
            │   - Inconsistent state across systems                        │
            │                                                              │
            │ With transactions:                                           │
            │   - All 3 writes succeed OR none succeed                     │
            │   - Atomic, consistent state                                 │
            └──────────────────────────────────────────────────────────────┘

            Transaction Configuration:
            ┌──────────────────────────────────────────────────────────────┐
            │ transactional.id = "order-processor-1"                       │
            │   - Unique per producer instance                             │
            │   - Survives restarts for recovery                           │
            │                                                              │
            │ enable.idempotence = true  (automatically enabled)           │
            │ acks = all                 (automatically set)               │
            │ max.in.flight.requests.per.connection = 5 (max for EOS)      │
            └──────────────────────────────────────────────────────────────┘
            """);
    }

    private void demonstrateBasicTransaction() throws Exception {
        explain("2. BASIC TRANSACTION: Single topic, multiple messages");

        step("Creating transactional producer...");

        Map<String, Object> txnProps = new HashMap<>(baseProps);
        txnProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "lesson06-txn-" + UUID.randomUUID());
        txnProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(txnProps);
        KafkaTemplate<String, String> txnTemplate = new KafkaTemplate<>(factory);

        // Initialize transactions (required before first transaction)
        txnTemplate.executeInTransaction(operations -> {
            result("Transaction initialized");
            return null;
        });

        step("Executing transaction with multiple messages...");

        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8);

        // Execute transaction
        String txnResult = txnTemplate.executeInTransaction(operations -> {
            System.out.println("    [TXN] Begin transaction");

            // Send multiple messages atomically
            operations.send(TOPIC_ORDERS, orderId, "{\"status\":\"CREATED\"}");
            System.out.println("    [TXN] Sent order creation");

            operations.send(TOPIC_ORDERS, orderId, "{\"status\":\"VALIDATED\"}");
            System.out.println("    [TXN] Sent order validation");

            operations.send(TOPIC_ORDERS, orderId, "{\"status\":\"PROCESSING\"}");
            System.out.println("    [TXN] Sent order processing");

            System.out.println("    [TXN] Committing transaction...");
            return "Transaction committed for order: " + orderId;
        });

        success(txnResult);
        result("All 3 messages are atomically visible to consumers");

        txnTemplate.destroy();
    }

    private void demonstrateMultiTopicTransaction() throws Exception {
        explain("3. MULTI-TOPIC TRANSACTION: Atomic writes across topics");

        step("Creating transactional producer for order processing...");

        Map<String, Object> txnProps = new HashMap<>(baseProps);
        txnProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "lesson06-multi-" + UUID.randomUUID());

        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(txnProps);
        KafkaTemplate<String, String> txnTemplate = new KafkaTemplate<>(factory);

        String orderId = "ORD-MULTI-" + UUID.randomUUID().toString().substring(0, 8);
        String productId = "PROD-001";
        String customerId = "CUST-001";

        step("Processing order with atomic multi-topic writes...");

        System.out.println("""

            Order Processing Pipeline:
            ┌─────────────────────────────────────────────────────────────┐
            │                                                             │
            │   ┌─────────────┐    ┌─────────────┐    ┌──────────────┐   │
            │   │   Orders    │    │  Inventory  │    │ Notifications│   │
            │   │   Topic     │    │   Topic     │    │    Topic     │   │
            │   └──────┬──────┘    └──────┬──────┘    └──────┬───────┘   │
            │          │                  │                  │           │
            │          └──────────────────┴──────────────────┘           │
            │                       │                                    │
            │                  Transaction                               │
            │                (all or nothing)                            │
            │                                                             │
            └─────────────────────────────────────────────────────────────┘
            """);

        txnTemplate.executeInTransaction(operations -> {
            System.out.println("    [TXN] Begin multi-topic transaction");

            // 1. Create order record
            String orderEvent = String.format(
                    "{\"orderId\":\"%s\",\"customerId\":\"%s\",\"productId\":\"%s\",\"quantity\":2,\"status\":\"CONFIRMED\"}",
                    orderId, customerId, productId);
            operations.send(TOPIC_ORDERS, orderId, orderEvent);
            System.out.println("    [TXN] -> Orders topic: Order confirmed");

            // 2. Update inventory
            String inventoryEvent = String.format(
                    "{\"productId\":\"%s\",\"adjustment\":-2,\"reason\":\"Order %s\"}",
                    productId, orderId);
            operations.send(TOPIC_INVENTORY, productId, inventoryEvent);
            System.out.println("    [TXN] -> Inventory topic: Stock decreased");

            // 3. Send notification
            String notificationEvent = String.format(
                    "{\"customerId\":\"%s\",\"type\":\"ORDER_CONFIRMATION\",\"orderId\":\"%s\"}",
                    customerId, orderId);
            operations.send(TOPIC_NOTIFICATIONS, customerId, notificationEvent);
            System.out.println("    [TXN] -> Notifications topic: Email queued");

            System.out.println("    [TXN] Committing multi-topic transaction...");
            return null;
        });

        success("Multi-topic transaction committed!");
        result("All 3 topics updated atomically");
        result("Consumers with read_committed isolation see all or none");

        txnTemplate.destroy();
    }

    private void demonstrateTransactionAbort() throws Exception {
        explain("4. TRANSACTION ABORT: Rollback on failure");

        step("Demonstrating transaction abort scenario...");

        Map<String, Object> txnProps = new HashMap<>(baseProps);
        txnProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "lesson06-abort-" + UUID.randomUUID());

        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(txnProps);
        KafkaTemplate<String, String> txnTemplate = new KafkaTemplate<>(factory);

        String orderId = "ORD-ABORT-" + UUID.randomUUID().toString().substring(0, 8);

        System.out.println("""

            Abort Scenario:
            ┌─────────────────────────────────────────────────────────────┐
            │ 1. Begin transaction                                        │
            │ 2. Send to orders topic         ✓ (buffered)               │
            │ 3. Send to inventory topic      ✓ (buffered)               │
            │ 4. Business validation FAILS    ✗                          │
            │ 5. Abort transaction                                        │
            │ 6. Messages 2 & 3 are DISCARDED                            │
            └─────────────────────────────────────────────────────────────┘
            """);

        try {
            txnTemplate.executeInTransaction(operations -> {
                System.out.println("    [TXN] Begin transaction");

                operations.send(TOPIC_ORDERS, orderId, "{\"status\":\"PENDING\"}");
                System.out.println("    [TXN] Sent order (buffered)");

                operations.send(TOPIC_INVENTORY, "PROD-001", "{\"adjustment\":-5}");
                System.out.println("    [TXN] Sent inventory update (buffered)");

                // Simulate business logic failure
                boolean inventoryAvailable = false; // Simulated check

                if (!inventoryAvailable) {
                    System.out.println("    [TXN] Business validation failed - insufficient inventory");
                    throw new RuntimeException("Insufficient inventory - aborting transaction");
                }

                return null;
            });
        } catch (Exception e) {
            System.out.println("    [TXN] Transaction aborted: " + e.getCause().getMessage());
            success("Transaction rolled back - no messages visible to consumers");
        }

        result("Aborted messages are marked with abort markers");
        result("Consumers with read_committed skip aborted messages");

        txnTemplate.destroy();

        tip("Transaction abort is automatic on exception in executeInTransaction()");
        tip("Consumers should use isolation.level=read_committed for EOS");
    }
}
