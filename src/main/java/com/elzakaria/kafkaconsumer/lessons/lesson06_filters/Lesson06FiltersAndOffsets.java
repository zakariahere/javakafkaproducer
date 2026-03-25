package com.elzakaria.kafkaconsumer.lessons.lesson06_filters;

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
import java.util.Map;

/**
 * Lesson 06: Consumer Filters & Offset Configuration (earliest/latest)
 *
 * Learn how to filter messages and control what offset consumers start from.
 * This lesson covers:
 * 1. Consumer filters - Processing only messages that match criteria
 * 2. auto.offset.reset = "earliest" - Start from beginning of partition
 * 3. auto.offset.reset = "latest" - Start from end of partition
 * 4. When each strategy is useful
 * 5. Combining filters with offset strategies
 */
@Component
public class Lesson06FiltersAndOffsets implements ConsumerLesson {
    private static final int NUM_PARTITIONS = 1;
    private static final String TOPIC = "lesson06-filters-offsets";
    private static final String GROUP_ID = "lesson06-group";

    @Override
    public int getLessonNumber() {
        return 6;
    }

    @Override
    public String getTitle() {
        return "Consumer Filters & Offset Configuration (earliest/latest)";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers filtering messages and controlling consumer starting position:

            1. CONSUMER FILTERS: Processing only relevant messages
               - Check message properties before processing
               - Skip messages that don't match criteria
               - Use case: Process only high-priority orders, specific regions, etc.

            2. auto.offset.reset = "earliest": Start from partition beginning
               - Resets offset to 0 (beginning) if consumer group has no stored offset
               - Reprocesses all historical data
               - Use case: New consumer group, data migration, backfill, compliance audits

            3. auto.offset.reset = "latest": Start from partition end
               - Resets offset to latest message if consumer group has no stored offset
               - Ignores all historical data
               - Use case: Real-time monitoring, new deployments, skip backlog

            4. OFFSET RESET TRIGGERS:
               - Consumer group is new (no committed offset)
               - Consumer group offset is invalid (too old, beyond current)
               - Consumer group explicitly seeks new position

            5. COMBINING FILTERS + OFFSETS:
               - Start from beginning and filter: Process all historical data selectively
               - Start from end and filter: Real-time processing of specific message types
            """;
    }

    @Override
    public void run() throws Exception {
        ConsumerUtils.createTopics(NUM_PARTITIONS, TOPIC);

        produceTestMessages();
        demonstrateConsumerFilters();
        demonstrateEarliestOffsetStrategy();
        demonstrateLatestOffsetStrategy();
        demonstreCombiningFiltersWithOffsets();

        tip("Use 'earliest' for backfill/replay scenarios, 'latest' for real-time monitoring");
        tip("Always ensure filtered messages are processed correctly - don't lose data unintentionally");
        tip("Consumer filters happen at application level - broker receives all messages anyway");
    }

    private void produceTestMessages() throws Exception {
        explain("Step 0: Producing diverse test messages to topic '" + TOPIC + "'");

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            // Produce diverse messages with different types
            String[] types = {"ORDER", "ORDER", "NOTIFICATION", "ORDER", "ALERT", "ORDER", "NOTIFICATION", "ALERT", "ORDER", "NOTIFICATION"};
            String[] regions = {"US", "EU", "ASIA", "US", "EU", "ASIA", "US", "EU", "ASIA", "US"};

            for (int i = 1; i <= 10; i++) {
                String type = types[i - 1];
                String region = regions[i - 1];
                String message = String.format("[%s] Region: %s | Message #%d | Timestamp: %d", 
                    type, region, i, System.currentTimeMillis());

                producer.send(new ProducerRecord<>(TOPIC, "msg-" + i, message));
            }
            producer.flush();
            result("Produced 10 diverse messages (3 ORDER, 3 NOTIFICATION, 2 ALERT, 2 others)");
        }
    }

    private void demonstrateConsumerFilters() throws Exception {
        explain("1. CONSUMER FILTERS: Processing only specific message types");

        step("Consuming all messages and filtering for ORDER type...");

        String groupId = GROUP_ID + "-filter-demo";
        Map<String, Object> consumerProps = createConsumerProps(groupId, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            result("Received " + records.count() + " total messages");

            step("Applying filter: Only processing ORDER messages...");
            int[] ordersProcessed = {0};
            System.out.println("\n  Filtered messages (ORDER type only):");

            records.forEach(record -> {
                String value = record.value();
                // Filter: only process messages containing "[ORDER]"
                if (value.contains("[ORDER]")) {
                    ordersProcessed[0]++;
                    System.out.println("    ✓ " + value);
                } else {
                    System.out.println("    ✗ SKIPPED (not an ORDER): " + value);
                }
            });

            success("Filtered results: " + ordersProcessed[0] + " ORDER messages out of " + records.count() + " total");

            tip("Filters run at application level - messages already transmitted from broker");
            tip("Use topic partitioning or server-side filtering for better performance with many messages");
        }
    }

    private void demonstrateEarliestOffsetStrategy() throws Exception {
        explain("2. OFFSET STRATEGY: auto.offset.reset = 'earliest'");

        step("Creating consumer with earliest offset reset...");

        String groupId = GROUP_ID + "-earliest-demo";
        Map<String, Object> consumerProps = createConsumerProps(groupId, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            result("Consumer group: " + groupId);
            result("Offset reset policy: EARLIEST");
            result("Expected behavior: Read from beginning of partition (offset 0)");

            step("Polling for messages (first time - should read all messages)...");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            result("First poll: Received " + records.count() + " messages starting from offset 0");

            System.out.println("\n  Messages (earliest strategy):");
            int[] count = {0};
            records.forEach(record -> {
                count[0]++;
                System.out.println("    [" + count[0] + "] Offset: " + record.offset() + " | " + record.value());
            });

            consumer.commitSync();
            result("Committed offsets");

            step("Second poll (should get new messages, if any)...");
            ConsumerRecords<String, String> records2 = consumer.poll(Duration.ofSeconds(1));
            result("Second poll: Received " + records2.count() + " new messages");

            success("With 'earliest' strategy, new consumer groups read entire history from start");

            tip("Use 'earliest' for: Data migration, backfill, replaying historical events");
            tip("Use 'earliest' for: Compliance audits, new consumer deployments");
        }
    }

    private void demonstrateLatestOffsetStrategy() throws Exception {
        explain("3. OFFSET STRATEGY: auto.offset.reset = 'latest'");

        step("Creating consumer with latest offset reset...");

        String groupId = GROUP_ID + "-latest-demo";
        Map<String, Object> consumerProps = createConsumerProps(groupId, "latest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            result("Consumer group: " + groupId);
            result("Offset reset policy: LATEST");
            result("Expected behavior: Start after last message in partition");

            step("Polling for messages (first time - should get 0 messages since all exist before now)...");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            result("First poll: Received " + records.count() + " messages (expected 0 - starting from end)");

            if (records.isEmpty()) {
                success("As expected, no messages received with 'latest' strategy on existing data");
            } else {
                warning("Unexpected messages received with 'latest' strategy - check consumer configuration");
            }

            step("Now producing a NEW message to demonstrate 'latest' strategy...");

            Map<String, Object> producerProps = new HashMap<>();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                producer.send(new ProducerRecord<>(TOPIC, "new-msg-1", "[LATEST] New message after consumer started | Timestamp: " + System.currentTimeMillis()));
                producer.flush();
                result("Produced 1 new message");
            }

            step("Polling again for the new message...");
            ConsumerRecords<String, String> records2 = consumer.poll(Duration.ofSeconds(5));
            result("Second poll: Received " + records2.count() + " messages");

            records2.forEach(record -> {
                System.out.println("    Caught new message: " + record.value());
            });

            consumer.commitSync();

            success("With 'latest' strategy, consumer skips all existing messages and only reads NEW ones");

            tip("Use 'latest' for: Real-time monitoring, live dashboards, new deployments");
            tip("Use 'latest' for: Skipping backlog when deploying updated service");
        }
    }

    private void demonstreCombiningFiltersWithOffsets() throws Exception {
        explain("4. COMBINING FILTERS WITH OFFSET STRATEGIES");

        step("Scenario: Process only US region orders from historical data (earliest) or real-time (latest)");

        // Part A: Earliest + Filter = Process historical US orders
        String groupId1 = GROUP_ID + "-earliest-filter";
        Map<String, Object> props1 = createConsumerProps(groupId1, "earliest");

        try (KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(props1)) {
            consumer1.subscribe(Collections.singletonList(TOPIC));

            step("Part A: Earliest strategy + US region filter");
            result("Strategy: Read from beginning and filter for US + ORDER");

            ConsumerRecords<String, String> records1 = consumer1.poll(Duration.ofSeconds(5));

            int[] count1 = {0};
            System.out.println("\n  US Orders (from historical data):");
            records1.forEach(record -> {
                String value = record.value();
                if (value.contains("[ORDER]") && value.contains("US")) {
                    count1[0]++;
                    System.out.println("    ✓ " + value);
                }
            });

            success("Found " + count1[0] + " historical US orders");
            consumer1.commitSync();
        }

        // Part B: Latest + Filter = Process only future US orders
        String groupId2 = GROUP_ID + "-latest-filter";
        Map<String, Object> props2 = createConsumerProps(groupId2, "latest");

        try (KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(props2)) {
            consumer2.subscribe(Collections.singletonList(TOPIC));

            step("Part B: Latest strategy + US region filter (skips historical data)");
            result("Strategy: Start from end and only process future US orders");

            ConsumerRecords<String, String> records2 = consumer2.poll(Duration.ofSeconds(5));
            result("First poll: " + records2.count() + " messages (expected 0 - starting from end)");

            // Produce a new US order
            Map<String, Object> producerProps = new HashMap<>();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                producer.send(new ProducerRecord<>(TOPIC, "latest-us-order", "[ORDER] Region: US | New order for latest strategy | Timestamp: " + System.currentTimeMillis()));
                producer.flush();
            }

            ConsumerRecords<String, String> records2b = consumer2.poll(Duration.ofSeconds(2));
            int[] count2 = {0};
            System.out.println("\n  New US Orders (real-time with latest strategy):");
            records2b.forEach(record -> {
                String value = record.value();
                if (value.contains("[ORDER]") && value.contains("US")) {
                    count2[0]++;
                    System.out.println("    ✓ " + value);
                }
            });

            success("Real-time filter caught " + count2[0] + " new US order(s)");
            consumer2.commitSync();
        }

        tip("Combine strategies: earliest for backfill/migration, latest for real-time");
        tip("Filters + offsets allow flexible consumer behavior for different use cases");
    }

    private Map<String, Object> createConsumerProps(String groupId, String offsetStrategy) {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("enable.auto.commit", false);
        props.put("auto.offset.reset", offsetStrategy);  // "earliest" or "latest"
        return props;
    }
}

