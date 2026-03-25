package com.elzakaria.kafkaconsumer.lessons.lesson04_deserialization;

import com.elzakaria.kafkaconsumer.ConsumerUtils;
import com.elzakaria.kafkaconsumer.lessons.Lesson;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Lesson 04: Deserialization & Type Safety
 *
 * Handle different serialization formats safely.
 */
@Component
public class Lesson04Deserialization implements Lesson {

    private static final String TOPIC = "lesson04-deserialization";
    private static final String GROUP_ID = "lesson04-group";

    @Override
    public int getLessonNumber() {
        return 4;
    }

    @Override
    public String getTitle() {
        return "Deserialization & Type Safety";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers deserialization strategies and error handling:

            1. DESERIALIZER TYPES:
               - StringDeserializer: Simple text messages
               - JsonDeserializer: JSON objects with schema mapping
               - AvroDeserializer: Binary format with schema registry
               - Custom Deserializer: Domain-specific logic

            2. DESERIALIZATION ERRORS:
               - Malformed data: Invalid JSON, truncated message
               - Type mismatch: JSON doesn't match expected class
               - Encoding issues: Wrong character encoding
               - Poison pill: Message that always fails deserialization

            3. ERROR HANDLING:
               - Try-catch in consumer loop
               - DeserializationException callback
               - Dead Letter Topic (DLT) for failed messages
               - Default value fallback

            4. TYPE SAFETY:
               - Use generics: KafkaConsumer<String, MyClass>
               - Validate schema on deserialization
               - Version compatibility with schema evolution

            5. SCHEMA EVOLUTION:
               - Adding fields: New consumers handle old messages
               - Removing fields: Old consumers reject new messages
               - Renaming fields: Need careful planning
               - Use Avro for strict schema management
            """;
    }

    @Override
    public void run() throws Exception {
        ConsumerUtils.createTopics(2, TOPIC);

        produceVariousMessages();

        demonstrateStringDeserialization();
        demonstrateJsonDeserialization();
        demonstrateDeserializationErrors();
        demonstrateCustomDeserializer();
        demonstrateErrorRecovery();

        tip("Always validate deserialized objects");
        tip("Use schema registry (Avro) for schema evolution");
        warning("Deserialization errors can crash your consumer!");
        warning("Implement Dead Letter Topic pattern for error handling!");
    }

    private void produceVariousMessages() throws Exception {
        explain("Producing various message formats");

        var producerProps = new HashMap<String, Object>();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps)) {
            // Valid strings
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC, "key1", "Hello Kafka"));
            // Valid JSON
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                    TOPIC, "key2", "{\"id\":1,\"name\":\"Alice\"}"
            ));
            // Invalid JSON (will cause deserialization error)
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                    TOPIC, "key3", "{invalid json"
            ));
            producer.flush();
            result("Produced 3 messages");
        }
    }

    private void demonstrateStringDeserialization() throws Exception {
        explain("1. STRING DESERIALIZATION: Simple text messages");

        step("Reading string messages...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-string");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            result("Consumed " + records.count() + " messages:");
            records.forEach(record -> {
                System.out.println("    Key: " + record.key() + ", Value: " + record.value());
            });

            consumer.commitSync();
            success("String deserialization works for simple text");
        }
    }

    private void demonstrateJsonDeserialization() throws Exception {
        explain("2. JSON DESERIALIZATION: Structured objects from JSON");

        step("Creating custom JSON deserializer...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-json");
        props.put("value.deserializer", "org.springframework.kafka.support.serializer.JsonDeserializer");
        props.put("spring.json.trusted.packages", "*");

        result("JSON deserializer configured");
        result("Valid JSON: {\"id\":1,\"name\":\"Alice\"}");
        result("Invalid JSON: {invalid json} - will cause error");

        tip("Use JsonDeserializer from Spring Kafka");
        tip("Configure trusted packages to prevent deserialization attacks");
    }

    private void demonstrateDeserializationErrors() throws Exception {
        explain("3. DESERIALIZATION ERRORS: Handling malformed data");

        step("Consuming messages with error handling...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-errors");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            int attempts = 0;
            while (attempts < 10) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                    records.forEach(record -> {
                        try {
                            // Simulate JSON parsing
                            if (record.value().startsWith("{")) {
                                // Try to parse as JSON
                                if (!record.value().contains("}")) {
                                    throw new RuntimeException("Invalid JSON: " + record.value());
                                }
                            }
                            result("Successfully deserialized: " + record.value());
                        } catch (Exception e) {
                            error("Failed to deserialize: " + e.getMessage());
                            // Handle error: log, send to DLT, skip, etc.
                        }
                    });

                    consumer.commitSync();
                } catch (org.apache.kafka.common.errors.SerializationException e) {
                    error("Serialization exception: " + e.getMessage());
                    // This happens at Kafka consumer level
                } catch (Exception e) {
                    error("Unexpected error: " + e.getMessage());
                }
                attempts++;
            }

            warning("Deserialization errors are common with evolving schemas");
            warning("Must handle gracefully to prevent consumer crash");
        }
    }

    private void demonstrateCustomDeserializer() throws Exception {
        explain("4. CUSTOM DESERIALIZER: Domain-specific deserialization");

        result("Example custom deserializer for OrderEvent:");
        System.out.println("""
            public class OrderEventDeserializer implements Deserializer<OrderEvent> {
                @Override
                public OrderEvent deserialize(String topic, byte[] data) {
                    if (data == null) return null;
                    
                    try {
                        String json = new String(data, "UTF-8");
                        // Parse JSON and validate
                        ObjectMapper mapper = new ObjectMapper();
                        OrderEvent event = mapper.readValue(json, OrderEvent.class);
                        
                        // Custom validation
                        if (event.getOrderId() <= 0) {
                            throw new InvalidOrderException("Invalid order ID");
                        }
                        
                        return event;
                    } catch (Exception e) {
                        throw new SerializationException("Failed to deserialize", e);
                    }
                }
            }
            """);

        result("Benefits of custom deserializer:");
        result("  - Type safety with generics");
        result("  - Validation in deserializer");
        result("  - Error handling before consumer");
        result("  - Schema evolution support");

        tip("Custom deserializers are better than application-level parsing");
    }

    private void demonstrateErrorRecovery() throws Exception {
        explain("5. ERROR RECOVERY: Handling poison pills and dead messages");

        System.out.println("""
            Strategies for unrecoverable deserialization errors:

            ┌─────────────────────────────────────────────────────┐
            │ 1. DEAD LETTER TOPIC (DLT)                          │
            │    Send failed messages to quarantine topic          │
            │    Later investigate and fix                         │
            │                                                     │
            │ 2. SKIP AND LOG                                     │
            │    Skip poison pill, log offset and content          │
            │    Alert operations team                             │
            │                                                     │
            │ 3. DEFAULT VALUE                                    │
            │    Use default/null for failed deserialization      │
            │    Only if it's safe for your business logic        │
            │                                                     │
            │ 4. PAUSE AND ALERT                                  │
            │    Pause consumer, alert team                        │
            │    Manual intervention required                      │
            └─────────────────────────────────────────────────────┘
            """);

        result("Recommended: Dead Letter Topic pattern");
        result("1. Main consumer subscribes to main topic");
        result("2. On deserialization error, produce to DLT");
        result("3. Separate DLT processor analyzes failures");
        result("4. Once fixed, replay from DLT");

        tip("Always have a DLT strategy for production systems");
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

