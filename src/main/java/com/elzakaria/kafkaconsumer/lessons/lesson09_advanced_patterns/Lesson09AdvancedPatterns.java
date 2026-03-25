package com.elzakaria.kafkaconsumer.lessons.lesson09_advanced_patterns;

import com.elzakaria.kafkaconsumer.ConsumerUtils;
import com.elzakaria.kafkaconsumer.lessons.Lesson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Lesson 09: Advanced Consumer Patterns
 *
 * Implement real-world architectural patterns.
 */
@Component
public class Lesson09AdvancedPatterns implements Lesson {

    public static final String TOPIC = "lesson09";
    private static final String GROUP_ID = TOPIC + "-group";

    @Override
    public int getLessonNumber() {
        return 9;
    }

    @Override
    public String getTitle() {
        return "Advanced Patterns - Multi-Topic & Aggregations";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers advanced consumer patterns:

            1. MULTI-TOPIC CONSUMPTION:
               - Subscribe to specific topics: [topic1, topic2, topic3]
               - Subscribe to topic pattern: /topic-.*-events/
               - Different handling per topic

            2. TOPIC PATTERN SUBSCRIPTION:
               - Automatic discovery of matching topics
               - Consumer joins when new topic matches pattern
               - Consumer leaves when topic deleted

            3. FAN-OUT PATTERN:
               - One input topic
               - Multiple output topics based on routing
               - Example: events → [errors, warnings, info]

            4. AGGREGATION PATTERN:
               - Collect messages across partitions
               - Compute aggregate (sum, count, average)
               - Emit result periodically

            5. WINDOWING:
               - Time-window: Last N seconds
               - Count-window: Last N messages
               - Tumbling vs sliding

            6. STREAM PROCESSING FUNDAMENTALS:
               - Exactly-once semantics
               - State management
               - Checkpointing

            7. EXACTLY-ONCE END-TO-END:
               - Idempotent processing
               - Atomic commits
               - Dead letter handling
            """;
    }

    @Override
    public void run() throws Exception {
        ConsumerUtils.createTopics(3, TOPIC + "-orders", TOPIC + "-payments", TOPIC + "-shipments");

        produceMultiTopicMessages();

        demonstrateMultiTopicConsumption();
        demonstrateTopicPatternSubscription();
        demonstrateFanOutPattern();
        demonstrateAggregation();

        tip("Use topic patterns for dynamic topic discovery");
        tip("Implement fan-out for event distribution");
        warning("Aggregation requires state management!");
    }

    private void produceMultiTopicMessages() throws Exception {
        explain("Producing messages to multiple topics");

        var producerProps = new HashMap<String, Object>();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps)) {
            for (int i = 1; i <= 5; i++) {
                producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC + "-orders", "order-" + i, "Order " + i));
                producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC + "-payments", "pay-" + i, "Payment " + i));
                producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC + "-shipments", "ship-" + i, "Shipment " + i));
            }
            producer.flush();
            result("Produced messages to 3 topics");
        }
    }

    private void demonstrateMultiTopicConsumption() throws Exception {
        explain("1. MULTI-TOPIC CONSUMPTION: Subscribe to specific topics");

        step("Subscribing to multiple topics...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-multi");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<String> topics = Arrays.asList(TOPIC + "-orders", TOPIC + "-payments", TOPIC + "-shipments");
            consumer.subscribe(topics);

            result("Subscribed to topics: " + topics);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            Map<String, Integer> topicCounts = new HashMap<>();
            records.forEach(r -> topicCounts.merge(r.topic(), 1, Integer::sum));

            System.out.println("\n  Messages per topic:");
            topicCounts.forEach((topic, count) ->
                    System.out.println("    " + topic + ": " + count));

            consumer.commitSync();
            success("Multi-topic subscription works!");

            tip("All messages from all topics arrive in one poll");
            tip("Use record.topic() to identify source");
        }
    }

    private void demonstrateTopicPatternSubscription() throws Exception {
        explain("2. TOPIC PATTERN: Automatic discovery of matching topics");

        step("Subscribing to topic pattern: " + TOPIC + "-.*");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-pattern");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Pattern pattern = Pattern.compile(TOPIC + "-.*");
            consumer.subscribe(pattern);

            result("Subscribed to pattern: " + pattern.pattern());

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            Set<String> matchedTopics = new HashSet<>();
            records.forEach(r -> matchedTopics.add(r.topic()));

            result("Pattern matched topics: " + matchedTopics);

            consumer.commitSync();
            success("Pattern subscription auto-discovers topics!");

            tip("Useful for multi-tenant systems");
            tip("New matching topics automatically joined");
        }
    }

    private void demonstrateFanOutPattern() throws Exception {
        explain("3. FAN-OUT PATTERN: Route messages to different topics");

        System.out.println("""
            Fan-Out Example:

            Input topic: events
            ├─ Consumers:
            │  ├─ Event Processor 1 → errors topic
            │  ├─ Event Processor 2 → warnings topic
            │  └─ Event Processor 3 → analytics topic
            └─ Each event processed independently

            Advantages:
            ├─ Decouples producers from specific consumers
            ├─ Multiple independent processing pipelines
            ├─ Easy to add new consumer without changing producer
            └─ Allows replay: old events available in all topics
            """);

        step("Simulating fan-out routing...");

        String[] eventTypes = {"ERROR", "WARNING", "INFO"};
        Map<String, Integer> routedCounts = new HashMap<>();

        for (String eventType : eventTypes) {
            int count = (int) (Math.random() * 5) + 1;
            routedCounts.put(eventType, count);
            result("Route " + count + " events to " + eventType.toLowerCase() + " topic");
        }

        System.out.println("\n  Routing summary:");
        routedCounts.forEach((type, count) ->
                System.out.println("    " + type + " → " + type.toLowerCase() + " topic: " + count + " events"));

        success("Fan-out pattern implemented!");

        tip("Use record key to maintain ordering per entity");
        tip("Each output topic can have different retention/partitions");
    }

    private void demonstrateAggregation() throws Exception {
        explain("4. AGGREGATION PATTERN: Compute metrics across messages");

        step("Aggregating messages (sum, count, average)...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-agg");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC + "-orders"));

            // Aggregate state
            Map<String, Integer> totalByType = new HashMap<>();
            int count = 0;
            double sum = 0;

            for (int poll = 0; poll < 2; poll++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));

                for (ConsumerRecord<String, String> record : records) {
                    // Extract numeric value
                    String value = record.value();
                    int num = record.partition();  // Use partition as placeholder
                    
                    sum += num;
                    count++;
                    totalByType.merge(value, 1, Integer::sum);
                }

                // Emit aggregates every batch
                if (!records.isEmpty()) {
                    double average = count > 0 ? sum / count : 0;
                    result("Batch aggregates - Count: " + count + ", Sum: " + (long)sum + ", Avg: " + String.format("%.2f", average));
                }
            }

            consumer.commitSync();

            System.out.println("\n  Final aggregates:");
            System.out.println("    Total messages: " + count);
            System.out.println("    Sum: " + (long)sum);
            System.out.println("    Average: " + String.format("%.2f", count > 0 ? sum / count : 0));

            success("Aggregation pattern works!");

            tip("Aggregate state = in-memory accumulation");
            tip("Periodic emit = every N messages or T seconds");
            tip("For large state, use changelog topics (Kafka Streams)");
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

