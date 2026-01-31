package com.elzakaria.kafkaproducer.lessons.lesson02_serialization;

import com.elzakaria.kafkaproducer.lessons.Lesson;
import com.elzakaria.kafkaproducer.model.Order;
import com.elzakaria.kafkaproducer.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Lesson 02: Serialization Patterns
 *
 * Learn different ways to serialize messages for Kafka:
 * 1. String serialization (simple)
 * 2. JSON serialization (flexible, human-readable)
 * 3. Custom serialization (full control)
 */
@Component
public class Lesson02Serialization implements Lesson {

    private static final String TOPIC = "lesson02-serialization";

    private final KafkaTemplate<String, String> stringKafkaTemplate;
    private final KafkaTemplate<String, Object> jsonKafkaTemplate;
    private final ObjectMapper objectMapper;

    public Lesson02Serialization(
            KafkaTemplate<String, String> stringKafkaTemplate,
            KafkaTemplate<String, Object> jsonKafkaTemplate) {
        this.stringKafkaTemplate = stringKafkaTemplate;
        this.jsonKafkaTemplate = jsonKafkaTemplate;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules(); // For Java 8 date/time support
    }

    @Override
    public int getLessonNumber() {
        return 2;
    }

    @Override
    public String getTitle() {
        return "Serialization - String, JSON, and Custom";
    }

    @Override
    public String getDescription() {
        return """
            This lesson explores message serialization options:

            1. STRING SERIALIZER: Simple text messages
               - Built-in, no configuration needed
               - Best for: simple strings, pre-serialized data

            2. JSON SERIALIZER: Objects to JSON
               - Human-readable, schema evolution friendly
               - Best for: most business objects

            3. MANUAL JSON: Control over serialization
               - Full control over format
               - Best for: when you need custom handling

            Note: Avro serialization requires Schema Registry (not covered here)
            """;
    }

    @Override
    public void run() throws Exception {
        demonstrateStringSerializer();
        demonstrateJsonSerializer();
        demonstrateManualJsonSerialization();

        tip("JSON is great for development; consider Avro for production schemas.");
        tip("Always include type hints in JSON to help consumers deserialize.");
    }

    private void demonstrateStringSerializer() throws Exception {
        explain("1. STRING SERIALIZER: The simplest approach");

        step("Sending plain string messages...");

        SendResult<String, String> result = stringKafkaTemplate.send(
                TOPIC + "-string",
                "simple-key",
                "This is a simple string message"
        ).get();

        success("String message sent to partition " + result.getRecordMetadata().partition());

        step("Sending CSV-formatted data as string...");

        String csvData = "orderId,product,quantity,price\nORD-001,Widget,10,29.99";
        stringKafkaTemplate.send(TOPIC + "-string", "csv-data", csvData).get();

        success("CSV data sent as string");
        result("Use case: Log lines, pre-formatted data, legacy integration");
    }

    private void demonstrateJsonSerializer() throws Exception {
        explain("2. JSON SERIALIZER: Automatic object-to-JSON conversion");

        step("Sending User object with JsonSerializer...");

        User user = User.builder()
                .userId("USR-" + UUID.randomUUID().toString().substring(0, 8))
                .name("John Doe")
                .email("john.doe@example.com")
                .department("Engineering")
                .build();

        SendResult<String, Object> userResult = jsonKafkaTemplate.send(
                TOPIC + "-json",
                user.getUserId(),
                user
        ).get();

        success("User object serialized to JSON");
        result("User: " + user);
        result("Partition: " + userResult.getRecordMetadata().partition());

        step("Sending Order object with JsonSerializer...");

        Order order = Order.builder()
                .orderId("ORD-" + UUID.randomUUID().toString().substring(0, 8))
                .customerId(user.getUserId())
                .product("Kafka Learning Guide")
                .quantity(1)
                .price(new BigDecimal("49.99"))
                .status("PENDING")
                .createdAt(Instant.now())
                .build();

        SendResult<String, Object> orderResult = jsonKafkaTemplate.send(
                TOPIC + "-json",
                order.getOrderId(),
                order
        ).get();

        success("Order object serialized to JSON");
        result("Order: " + order);
        result("Note: BigDecimal and Instant are properly serialized");

        step("Sending a Map as JSON (schema-less)...");

        Map<String, Object> event = new HashMap<>();
        event.put("eventType", "USER_LOGIN");
        event.put("userId", user.getUserId());
        event.put("timestamp", System.currentTimeMillis());
        event.put("metadata", Map.of("ip", "192.168.1.1", "userAgent", "Mozilla/5.0"));

        jsonKafkaTemplate.send(TOPIC + "-json", "event-" + System.currentTimeMillis(), event).get();

        success("Map serialized to JSON - useful for dynamic schemas");
    }

    private void demonstrateManualJsonSerialization() throws Exception {
        explain("3. MANUAL JSON: Full control over serialization");

        step("Manually serializing object to JSON string...");

        Order order = Order.builder()
                .orderId("ORD-MANUAL-001")
                .customerId("CUST-001")
                .product("Manual Serialization Example")
                .quantity(5)
                .price(new BigDecimal("99.99"))
                .status("CREATED")
                .createdAt(Instant.now())
                .build();

        // Manually convert to JSON
        String jsonString = objectMapper.writeValueAsString(order);

        result("Manual JSON: " + jsonString);

        // Send as string
        stringKafkaTemplate.send(TOPIC + "-manual", order.getOrderId(), jsonString).get();

        success("Manually serialized JSON sent as string");

        step("Adding wrapper with type information...");

        // Create a wrapper with type info for consumers
        Map<String, Object> wrapper = new HashMap<>();
        wrapper.put("type", "Order");
        wrapper.put("version", "1.0");
        wrapper.put("payload", order);
        wrapper.put("timestamp", Instant.now().toString());

        String wrappedJson = objectMapper.writeValueAsString(wrapper);
        stringKafkaTemplate.send(TOPIC + "-manual", "wrapped-" + order.getOrderId(), wrappedJson).get();

        success("Wrapped JSON with type info sent");
        tip("Type wrappers help consumers handle multiple message types on the same topic");
    }
}
