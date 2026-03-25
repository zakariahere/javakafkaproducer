package com.elzakaria.kafkaconsumer.lessons.lesson01_basics;

import com.elzakaria.kafkaconsumer.ConsumerUtils;
import com.elzakaria.kafkaconsumer.lessons.ConsumerLesson;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Lesson 01: Consumer Basics - Polling & Message Consumption
 *
 * Learn the fundamental polling model and how consumers retrieve messages.
 * This lesson covers:
 * 1. Synchronous polling with poll(Duration)
 * 2. Consumer group registration and partition assignment
 * 3. Offset management (auto vs manual)
 * 4. Basic consumer properties
 * 5. Message lifecycle
 */
@Component
public class Lesson01ConsumerBasics implements ConsumerLesson {

    private static final String TOPIC = "lesson01-consumer-basics";
    private static final String GROUP_ID = "lesson01-group";
    private static final int NUM_PARTITIONS = 2;

    @Override
    public int getLessonNumber() {
        return 1;
    }

    @Override
    public String getTitle() {
        return "Consumer Basics - Polling & Message Consumption";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers the fundamental polling model for consuming messages:

            1. POLLING MODEL: Consumer pulls messages from broker
               - Poll in a loop: while(true) { records = consumer.poll(duration) }
               - Non-blocking per message, blocking on poll timeout
               - Use case: All Kafka consumers work this way

            2. CONSUMER GROUPS: Logical grouping of consumers
               - Multiple consumers can share partition load
               - Each partition goes to exactly one consumer in a group
               - Enables horizontal scaling and fault tolerance

            3. OFFSET MANAGEMENT: Tracking what was consumed
               - Consumer remembers offset of last message processed
               - Auto-commit: Kafka commits automatically (less control)
               - Manual commit: Application controls when to commit (more safety)

            4. PARTITION ASSIGNMENT: How brokers allocate partitions
               - Static assignment: You manually specify partitions
               - Dynamic assignment: Consumer group coordinator assigns
               - Partitions are evenly distributed across consumers

            5. MESSAGE LIFECYCLE:
               Consumer fetches → Deserializes → Application processes → Commits offset
            """;
    }

    @Override
    public void run() throws Exception {
        // Create topics with proper partition count for demonstration
        ConsumerUtils.createTopics(NUM_PARTITIONS, TOPIC, TOPIC + "-group-demo", TOPIC + "-offset-demo");

        produceTestMessagesForPolling();
        demonstrateBasicPolling();

        produceTestMessagesForConsumerGroup();
        demonstrateConsumerGroup();

        produceTestMessagesForOffsetTracking();
        demonstrateOffsetTracking();

        tip("Always use consumer groups in production - provides scalability and fault tolerance");
        tip("Poll timeout of 100-1000ms is typical - balance between latency and CPU");
    }

    private void produceTestMessagesForPolling() throws Exception {
        explain("Step 0: Producing test messages to topic '" + TOPIC + "'");

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 1; i <= 10; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "key-" + i, "Message #" + i + " at " + System.currentTimeMillis())).get();
                step("Produced message #" + i);
            }
            producer.flush();
            result("Produced 10 test messages");
        }
    }

    private void produceTestMessagesForConsumerGroup() throws Exception {
        String topicForGroup = TOPIC + "-group-demo";
        explain("Step 0b: Producing test messages to topic '" + topicForGroup + "' (" + NUM_PARTITIONS + " partitions)");

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            // Send 10 messages - distributed across partitions via round-robin (due to different keys)
            for (int i = 1; i <= 10; i++) {
                String key = "key-" + i;
                String value = "Message #" + i + " for group demo";
                producer.send(new ProducerRecord<>(topicForGroup, key, value)).get();
            }
            producer.flush();
            result("Produced 10 messages distributed across " + NUM_PARTITIONS + " partitions");
        }
    }

    private void produceTestMessagesForOffsetTracking() throws Exception {
        String topicForOffsets = TOPIC + "-offset-demo";
        explain("Step 0c: Producing test messages to topic '" + topicForOffsets + "'");

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 1; i <= 10; i++) {
                producer.send(new ProducerRecord<>(topicForOffsets, "key-" + i, "Message #" + i + " at " + System.currentTimeMillis())).get();
            }
            producer.flush();
            result("Produced 10 test messages");
        }
    }

    private void demonstrateBasicPolling() throws Exception {
        explain("1. BASIC POLLING: Synchronously consuming messages in a loop");

        step("Creating consumer and subscribing to topic...");

        Map<String, Object> consumerProps = createConsumerProps(GROUP_ID + "-basic");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            result("Consumer subscribed to topic: " + TOPIC);
            result("Consumer group: " + consumerProps.get("group.id"));

            step("Polling for messages...");
            tip("First poll may take 1-5 seconds as broker performs group coordination and partition assignment");

            int[] messagesConsumed = {0};  // Array wrapper for lambda
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            records.forEach(record -> {
                messagesConsumed[0]++;
                System.out.println("    Message: topic=" + record.topic() +
                    ", partition=" + record.partition() +
                    ", offset=" + record.offset() +
                    ", key=" + record.key() +
                    ", value=" + record.value());
            });

            success("Consumed " + messagesConsumed[0] + " messages");

            tip("poll() returns ConsumerRecords - a collection of messages from various partitions");
            tip("If no messages arrive within timeout, poll() returns empty collection (not null)");
        }
    }

    private void demonstrateConsumerGroup() throws Exception {
        explain("2. CONSUMER GROUP: Multiple consumers sharing partitions");

        step("Creating two consumers in same group to split " + NUM_PARTITIONS + " partitions...");

        String topicForGroup = TOPIC + "-group-demo";
        String groupId = GROUP_ID + "-group-demo";

        Map<String, Object> props1 = createConsumerProps(groupId);
        Map<String, Object> props2 = createConsumerProps(groupId);

        try (KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(props1);
             KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(props2)) {

            consumer1.subscribe(Collections.singletonList(topicForGroup));
            result("Consumer 1 subscribed to '" + topicForGroup + "'");

            consumer2.subscribe(Collections.singletonList(topicForGroup));
            result("Consumer 2 subscribed to '" + topicForGroup + "'");

            CompletableFuture<ConsumerRecords<String, String>> futureRecords1 = CompletableFuture.supplyAsync(() -> consumer1.poll(Duration.ofSeconds(7)));
            CompletableFuture<ConsumerRecords<String, String>> futureRecords2 = CompletableFuture.supplyAsync(() -> consumer2.poll(Duration.ofSeconds(7)));

            ConsumerRecords<String, String> records1 = futureRecords1.get();
            ConsumerRecords<String, String> records2 = futureRecords2.get();

            Set<Integer> partitions1 = new HashSet<>();
            Set<Integer> partitions2 = new HashSet<>();
            records1.forEach(record -> {
                partitions1.add(record.partition());
                System.out.println("    Consumer1 - Message: topic=" + record.topic() +
                    ", partition=" + record.partition() +
                    ", offset=" + record.offset() +
                    ", key=" + record.key() +
                    ", value=" + record.value());
            });

            records2.forEach(record -> {
                partitions2.add(record.partition());
                System.out.println("    Consumer2 - Message: topic=" + record.topic() +
                    ", partition=" + record.partition() +
                    ", offset=" + record.offset() +
                    ", key=" + record.key() +
                    ", value=" + record.value());
            });

            success("Partition assignment after rebalancing:");
            success("  Consumer 1 got partition(s): " + (partitions1.isEmpty() ? "none" : partitions1));
            success("  Consumer 2 got partition(s): " + (partitions2.isEmpty() ? "none" : partitions2));

            tip("With 2 consumers and 2 partitions, each consumer typically gets 1 partition");
            tip("Kafka coordinator automatically assigns partitions to balance load");
            tip("This demonstrates horizontal scaling - add more consumers to handle more load");
        }
    }

    private void demonstrateOffsetTracking() throws Exception {
        explain("3. OFFSET TRACKING: Understanding message position in partition");

        step("Consuming messages and tracking offsets...");

        String topicForOffsets = TOPIC + "-offset-demo";
        String groupId = GROUP_ID + "-offset-demo";

        Map<String, Object> consumerProps = createConsumerProps(groupId);
        consumerProps.put("auto.offset.reset", "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topicForOffsets));

            // Poll multiple times to allow consumer group coordination and offset initialization
            step("Polling for messages with multiple attempts for coordination...");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            System.out.println("\n  Offset information per partition:");
            records.forEach(record -> {
                System.out.println("    Partition " + record.partition() +
                    " @ Offset " + record.offset() +
                    ": " + record.value());
            });

            step("Current committed offsets (before manual commit)...");
            logCommitedOffset(consumer);

            step("Committing offsets manually...");

            // Manual commit - consumer remembers it processed up to this offset
            consumer.commitSync();
            success("Offsets committed to broker (stored in __consumer_offsets topic)");

            step("Current committed offsets...");
            logCommitedOffset(consumer);

            tip("Offset is the position of the next message to consume");
            tip("Committing tells Kafka: 'I've processed all messages up to this offset'");
            tip("If consumer crashes, it resumes from last committed offset");
        }
    }

    private void logCommitedOffset(KafkaConsumer<String, String> consumer) {
        var topicPartitions = consumer.assignment();
        for (var tp : topicPartitions) {
            var committed = consumer.committed(tp);
            if (committed != null) {
                result("Partition " + tp.partition() + ": committed offset = " + committed.offset());
            } else {
                result("Partition " + tp.partition() + ": no offset committed yet");
            }
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

        props.put("group.initial.rebalance.delay.ms", 100);
        return props;
    }
}

