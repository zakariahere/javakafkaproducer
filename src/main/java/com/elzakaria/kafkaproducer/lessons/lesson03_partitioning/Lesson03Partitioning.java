package com.elzakaria.kafkaproducer.lessons.lesson03_partitioning;

import com.elzakaria.kafkaproducer.lessons.Lesson;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Lesson 03: Partitioning Strategies
 *
 * Learn how Kafka distributes messages across partitions:
 * 1. Default partitioner (round-robin for null keys, hash for keys)
 * 2. Key-based partitioning (ordering guarantee)
 * 3. Custom partitioner (business logic routing)
 */
@Component
public class Lesson03Partitioning implements Lesson {

    private static final String TOPIC = "lesson03-partitioning";
    private static final int NUM_PARTITIONS = 4;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaAdmin kafkaAdmin;
    private final Map<String, Object> producerProps;

    public Lesson03Partitioning(KafkaTemplate<String, String> kafkaTemplate, KafkaAdmin kafkaAdmin) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaAdmin = kafkaAdmin;
        this.producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    @Override
    public int getLessonNumber() {
        return 3;
    }

    @Override
    public String getTitle() {
        return "Partitioning - Keys and Distribution";
    }

    @Override
    public String getDescription() {
        return """
            This lesson explains how messages are distributed across partitions:

            1. NULL KEY (Round-Robin): Messages spread evenly across partitions
               - No ordering guarantee
               - Best for: independent events, maximum throughput

            2. KEY-BASED: Same key always goes to same partition
               - Ordering guarantee per key
               - Best for: related events (same user, same order)

            3. EXPLICIT PARTITION: Direct partition assignment
               - Full control
               - Best for: special routing requirements

            Why partitions matter:
            - Parallelism: More partitions = more consumers can read concurrently
            - Ordering: Only guaranteed within a single partition
            - Scalability: Partitions spread load across brokers
            """;
    }

    @Override
    public void run() throws Exception {
        createTopicWithPartitions();

        demonstrateNullKeyPartitioning();
        demonstrateKeyBasedPartitioning();
        demonstrateExplicitPartitioning();
        demonstrateCustomPartitioner();

        tip("Use meaningful keys (userId, orderId) to keep related events together.");
        tip("More partitions = better parallelism, but also more overhead.");
    }

    private void createTopicWithPartitions() {
        explain("Creating topic with " + NUM_PARTITIONS + " partitions for demonstration");

        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            NewTopic topic = new NewTopic(TOPIC, NUM_PARTITIONS, (short) 1);
            adminClient.createTopics(Collections.singleton(topic));
            result("Topic '" + TOPIC + "' created with " + NUM_PARTITIONS + " partitions");
        } catch (Exception e) {
            result("Topic may already exist: " + e.getMessage());
        }
    }

    private void demonstrateNullKeyPartitioning() throws Exception {
        explain("1. NULL KEY: Round-robin distribution (sticky partitioner)");

        step("Sending messages without keys...");

        Map<Integer, Integer> partitionCounts = new HashMap<>();

        for (int i = 1; i <= 12; i++) {
            SendResult<String, String> result = kafkaTemplate.send(
                    TOPIC,
                    null,  // null key
                    "Message without key #" + i
            ).get();

            int partition = result.getRecordMetadata().partition();
            partitionCounts.merge(partition, 1, Integer::sum);

            result("Message #" + i + " -> Partition " + partition);
        }

        System.out.println("\n  Distribution summary:");
        partitionCounts.forEach((p, count) ->
                System.out.println("    Partition " + p + ": " + count + " messages"));

        tip("Sticky partitioner batches null-key messages to same partition for efficiency.");
    }

    private void demonstrateKeyBasedPartitioning() throws Exception {
        explain("2. KEY-BASED: Consistent partition assignment");

        step("Sending messages with user IDs as keys...");

        String[] users = {"user-alice", "user-bob", "user-charlie", "user-alice", "user-bob", "user-alice"};

        Map<String, Integer> userPartitions = new HashMap<>();

        for (String userId : users) {
            SendResult<String, String> result = kafkaTemplate.send(
                    TOPIC,
                    userId,  // key determines partition
                    "Event for " + userId + " at " + System.currentTimeMillis()
            ).get();

            int partition = result.getRecordMetadata().partition();
            userPartitions.put(userId, partition);

            result(userId + " -> Partition " + partition);
        }

        System.out.println("\n  Key-to-partition mapping:");
        userPartitions.forEach((user, partition) ->
                System.out.println("    " + user + " always goes to partition " + partition));

        success("Same key = same partition = guaranteed ordering for that key");

        step("Demonstrating ordering guarantee...");

        String orderId = "order-12345";
        String[] orderEvents = {"CREATED", "PAYMENT_RECEIVED", "SHIPPED", "DELIVERED"};

        for (String event : orderEvents) {
            SendResult<String, String> result = kafkaTemplate.send(TOPIC, orderId, event).get();
            result("Order " + orderId + " [" + event + "] -> Partition " +
                    result.getRecordMetadata().partition() + ", Offset " +
                    result.getRecordMetadata().offset());
        }

        success("All events for order-12345 went to same partition - ordering preserved!");
    }

    private void demonstrateExplicitPartitioning() throws Exception {
        explain("3. EXPLICIT PARTITION: Direct assignment");

        step("Sending messages to specific partitions...");

        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
            SendResult<String, String> result = kafkaTemplate.send(
                    TOPIC,
                    partition,  // explicit partition
                    "explicit-key",
                    "Message explicitly sent to partition " + partition
            ).get();

            result("Sent to partition " + result.getRecordMetadata().partition() +
                    " (requested: " + partition + ")");
        }

        success("Explicit partitioning gives you full control");
        tip("Use explicit partitioning for priority queues or special routing.");
    }

    private void demonstrateCustomPartitioner() throws Exception {
        explain("4. CUSTOM PARTITIONER: Business logic routing");

        step("Using a region-based custom partitioner...");

        // Create a template with custom partitioner
        Map<String, Object> props = new HashMap<>(producerProps);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RegionPartitioner.class.getName());

        DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);
        KafkaTemplate<String, String> customTemplate = new KafkaTemplate<>(factory);

        // Send messages with region prefixes
        String[] regions = {"US-order-001", "EU-order-002", "APAC-order-003", "US-order-004", "EU-order-005"};

        for (String key : regions) {
            SendResult<String, String> result = customTemplate.send(
                    TOPIC,
                    key,
                    "Order from " + key
            ).get();

            result(key + " -> Partition " + result.getRecordMetadata().partition());
        }

        customTemplate.destroy();

        System.out.println("\n  Custom partitioner logic:");
        System.out.println("    US-*   -> Partition 0");
        System.out.println("    EU-*   -> Partition 1");
        System.out.println("    APAC-* -> Partition 2");
        System.out.println("    Other  -> Partition 3");

        tip("Custom partitioners enable geo-routing, priority handling, and tenant isolation.");
    }
}
