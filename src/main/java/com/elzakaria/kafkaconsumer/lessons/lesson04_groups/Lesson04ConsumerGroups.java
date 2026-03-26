package com.elzakaria.kafkaconsumer.lessons.lesson04_groups;

import com.elzakaria.kafkaconsumer.ConsumerUtils;
import com.elzakaria.kafkaconsumer.lessons.ConsumerLesson;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Lesson 04: Consumer Groups & Rebalancing
 *
 * Master consumer groups, partition allocation, and rebalancing strategies.
 */
@Component
public class Lesson04ConsumerGroups implements ConsumerLesson {

    private static final String TOPIC = "lesson04-consumer-groups";
    private static final String GROUP_ID = "lesson04-group";
    private static final int NUM_PARTITIONS = 4;

    @Override
    public int getLessonNumber() {
        return 4;
    }

    @Override
    public String getTitle() {
        return "Consumer Groups & Rebalancing";
    }

    @Override
    public String getDescription() {
        return """
            This lesson explains consumer groups and the rebalancing process:

            1. CONSUMER GROUP: Multiple consumers sharing partition load
               - All consumers with same group.id form a group
               - Kafka distributes partitions among group members
               - Enables horizontal scaling and fault tolerance

            2. PARTITION ASSIGNMENT STRATEGIES:
               - RangeAssignor: Partitions assigned in range order (bias to early consumers)
               - RoundRobinAssignor: Partitions distributed round-robin
               - StickyAssignor: Tries to minimize partition movement on rebalance
               - CooperativeStickyAssignor: Sticky + can revoke only needed partitions

            3. REBALANCING: Redistribution of partitions when group changes
               - TRIGGERED BY: New consumer joins, consumer leaves/crashes, topic changes
               - STOPS PROCESSING: All consumers pause to rebalance (Stop the World)
               - DURATION: Typically 3-30 seconds depending on workload
               - LISTENERS: RebalanceListener hooks into rebalance events

            4. SESSION & HEARTBEAT: Keeping consumer alive
               - session.timeout.ms: How long broker waits for heartbeat before removing
               - heartbeat.interval.ms: How often to send heartbeat
               - max.poll.interval.ms: Max time between polls before consumer evicted

            5. IMPLICATIONS:
               - Message processing pauses during rebalancing
               - Consumer lag increases during rebalancing
               - Frequent rebalances = instability (stop the world problem)
            """;
    }

    @Override
    public void run() throws Exception {
        ConsumerUtils.createTopics(NUM_PARTITIONS, TOPIC);

        produceMessages();

        demonstratePartitionAssignment();
        demonstrateRebalancing();
        demonstrateRebalanceListener();
        demonstrateSessionTimeout();

        tip("Use 'sticky' or 'cooperative-sticky' assignors to minimize rebalance impact");
        tip("Monitor rebalance time with metrics - high values indicate problems");
        warning("Long message processing can trigger rebalancing (max.poll.interval.ms)");
    }

    private void produceMessages() throws Exception {
        explain("Producing messages across partitions");

        var producerProps = new HashMap<String, Object>();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps)) {
            for (int i = 0; i < 20; i++) {
                producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                        TOPIC, "key-" + i, "Message " + i
                ));
            }
            producer.flush();
            result("Produced 20 messages");
        }
    }

    private void demonstratePartitionAssignment() throws Exception {
        explain("1. PARTITION ASSIGNMENT: How Kafka distributes partitions to consumers");

        step("Single consumer consuming from all partitions...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-single");
        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            Set<Integer> assignedPartitions = new HashSet<>();

            records.forEach(r -> assignedPartitions.add(r.partition()));

            success("Single consumer assigned partitions: " + assignedPartitions);
            result("Consumer got all " + NUM_PARTITIONS + " partitions");

            tip("With 1 consumer in group, it gets all partitions");
        }

        step("Multiple consumers with different assignors...");

        String[] assignors = {
                "org.apache.kafka.clients.consumer.RangeAssignor",
                "org.apache.kafka.clients.consumer.RoundRobinAssignor",
                "org.apache.kafka.clients.consumer.StickyAssignor"
        };

        for (String assignor : assignors) {
            demonstrateAssignor(assignor);
        }
    }

    private void demonstrateAssignor(String assignorClass) throws Exception {
        String assignorName = assignorClass.substring(assignorClass.lastIndexOf('.') + 1);
        result("\nTesting " + assignorName + "...");

        Map<String, Object> props1 = createConsumerProps(GROUP_ID + "-" + assignorName);
        props1.put("partition.assignment.strategy", assignorClass);

        Map<String, Object> props2 = createConsumerProps(GROUP_ID + "-" + assignorName);
        props2.put("partition.assignment.strategy", assignorClass);

        try (KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(props1);
             KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(props2)) {

            consumer1.subscribe(Collections.singletonList(TOPIC));
            consumer2.subscribe(Collections.singletonList(TOPIC));

            CompletableFuture<ConsumerRecords<String, String>> futureRecords1 = CompletableFuture.supplyAsync(() -> consumer1.poll(Duration.ofSeconds(7)));
            CompletableFuture<ConsumerRecords<String, String>> futureRecords2 = CompletableFuture.supplyAsync(() -> consumer2.poll(Duration.ofSeconds(7)));

            Set<Integer> p1 = new HashSet<>();
            Set<Integer> p2 = new HashSet<>();

            futureRecords1.get().forEach(r -> p1.add(r.partition()));
            futureRecords2.get().forEach(r -> p2.add(r.partition()));

            result("  Consumer 1 partitions: " + p1);
            result("  Consumer 2 partitions: " + p2);
        }
    }

    private void demonstrateRebalancing() throws Exception {
        explain("2. REBALANCING: What happens when consumer joins/leaves group");

        step("Starting first consumer...");

        Map<String, Object> props1 = createConsumerProps(GROUP_ID + "-rebalance-test");
        KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(props1);
        consumer1.subscribe(Collections.singletonList(TOPIC));

        long startTime = System.currentTimeMillis();
        ConsumerRecords<String, String> records1 = consumer1.poll(Duration.ofSeconds(5));
        Set<Integer> initial = getPartitionsFromRecords(records1);

        result("Consumer 1 assigned partitions: " + initial);

        step("Adding second consumer (triggers rebalance)...");

        Map<String, Object> props2 = createConsumerProps(GROUP_ID + "-rebalance-test");
        KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(props2);
        consumer2.subscribe(Collections.singletonList(TOPIC));
        consumer2.poll(Duration.ofSeconds(5));

        produceMessages();

        // Polling triggers rebalance detection
        CompletableFuture<ConsumerRecords<String, String>> futureRecords1 = CompletableFuture.supplyAsync(() -> consumer1.poll(Duration.ofSeconds(7)));
        CompletableFuture<ConsumerRecords<String, String>> futureRecords2 = CompletableFuture.supplyAsync(() -> consumer2.poll(Duration.ofSeconds(7)));

        Set<Integer> afterRebalance1 = new HashSet<>();
        Set<Integer> afterRebalance2 = new HashSet<>();

        futureRecords1.get().forEach(r -> afterRebalance1.add(r.partition()));
        futureRecords2.get().forEach(r -> afterRebalance2.add(r.partition()));

        result("After rebalance - Consumer 1 partitions: " + afterRebalance1);
        result("After rebalance - Consumer 2 partitions: " + afterRebalance2);

        success("Partitions redistributed between consumers!");

        consumer1.close();
        consumer2.close();

        tip("Rebalancing pauses all message processing (Stop the World)");
        tip("Frequent rebalances indicate instability - check consumer health");
    }

    private void demonstrateRebalanceListener() throws Exception {
        explain("3. REBALANCE LISTENER: Hooks into rebalance events");

        step("Creating consumer with RebalanceListener...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-listener-test");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        AtomicBoolean revokedCalled = new AtomicBoolean(false);
        AtomicBoolean assignedCalled = new AtomicBoolean(false);

        consumer.subscribe(Collections.singletonList(TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("    [REVOKED] Partitions being revoked: " + partitions);
                revokedCalled.set(true);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("    [ASSIGNED] New partitions assigned: " + partitions);
                assignedCalled.set(true);
            }
        });

        result("Polling to trigger rebalance listener...");
        consumer.poll(Duration.ofSeconds(5));

        if (revokedCalled.get()) {
            success("onPartitionsRevoked() was called!");
        } else {
            warning("onPartitionsRevoked() was NOT called - likely first assignment, no revocation");
        }
        if (assignedCalled.get()) {
            success("onPartitionsAssigned() was called!");
        } else {
            warning("onPartitionsAssigned() was NOT called - something is wrong");
        }

        tip("Use RebalanceListener to:");
        tip("  - Flush/commit state before revocation");
        tip("  - Load state from database on assignment");
        tip("  - Clean up resources");

        consumer.close();
    }

    private void demonstrateSessionTimeout() throws Exception {
        explain("4. SESSION TIMEOUT: Heartbeat and consumer liveness");

        System.out.println("""
            Key timeouts and heartbeat settings:

            ┌────────────────────────────────────────────────────────┐
            │ session.timeout.ms (default 10s)                       │
            │ → How long broker waits for heartbeat before removing  │
            │ → Consumer must send heartbeat within this time        │
            │                                                        │
            │ heartbeat.interval.ms (default 3s)                    │
            │ → How often to send heartbeat to coordinator           │
            │ → Should be 1/3 of session.timeout.ms                 │
            │                                                        │
            │ max.poll.interval.ms (default 5 min)                  │
            │ → Max time between poll() calls                        │
            │ → Consumer evicted if poll() not called in this time  │
            │ → Long message processing can trigger this!           │
            └────────────────────────────────────────────────────────┘
            """);

        result("Example 1: Normal operation");
        result("  Consumer polls every 100ms (< session.timeout.ms) -> stays alive");

        result("Example 2: Consumer hangs");
        result("  Consumer doesn't poll for 15s (> session.timeout.ms)");
        result("  → Broker removes consumer");
        result("  → Rebalance triggered");
        result("  → Consumer's partitions reassigned to others");

        warning("Don't do long operations without polling!");
        warning("If processing takes 30s, increase max.poll.interval.ms");
    }

    private Set<Integer> getPartitionsFromRecords(ConsumerRecords<String, String> records) {
        Set<Integer> partitions = new HashSet<>();
        records.forEach(r -> partitions.add(r.partition()));
        return partitions;
    }

    private Map<String, Object> createConsumerProps(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("enable.auto.commit", false);
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", 10000);
        props.put("heartbeat.interval.ms", 3000);
        return props;
    }
}

