package com.elzakaria.kafkaproducer.lessons.lesson11_avro;

import com.elzakaria.kafkaproducer.lessons.Lesson;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Lesson 11: Avro Serialization with Schema Registry
 *
 * Learn production-grade serialization with Apache Avro:
 * 1. Why Avro + Schema Registry
 * 2. Schema definition and evolution
 * 3. GenericRecord vs SpecificRecord
 * 4. Schema compatibility rules
 */
@Component
public class Lesson11Avro implements Lesson {

    private static final String TOPIC = "lesson11-avro";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    // Avro schema for Order (defined inline for clarity)
    private static final String ORDER_SCHEMA_V1 = """
            {
              "type": "record",
              "name": "Order",
              "namespace": "com.elzakaria.kafkaproducer.avro",
              "fields": [
                {"name": "orderId", "type": "string"},
                {"name": "customerId", "type": "string"},
                {"name": "product", "type": "string"},
                {"name": "quantity", "type": "int"},
                {"name": "totalPrice", "type": "double"},
                {"name": "status", "type": "string"},
                {"name": "timestamp", "type": "long"}
              ]
            }
            """;

    // Schema V2 with backward compatible changes (new optional field)
    private static final String ORDER_SCHEMA_V2 = """
            {
              "type": "record",
              "name": "Order",
              "namespace": "com.elzakaria.kafkaproducer.avro",
              "fields": [
                {"name": "orderId", "type": "string"},
                {"name": "customerId", "type": "string"},
                {"name": "product", "type": "string"},
                {"name": "quantity", "type": "int"},
                {"name": "totalPrice", "type": "double"},
                {"name": "status", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "shippingAddress", "type": ["null", "string"], "default": null}
              ]
            }
            """;

    // User event schema
    private static final String USER_EVENT_SCHEMA = """
            {
              "type": "record",
              "name": "UserEvent",
              "namespace": "com.elzakaria.kafkaproducer.avro",
              "fields": [
                {"name": "userId", "type": "string"},
                {"name": "email", "type": "string"},
                {"name": "eventType", "type": {"type": "enum", "name": "EventType", "symbols": ["CREATED", "UPDATED", "DELETED", "LOGIN"]}},
                {"name": "timestamp", "type": "long"},
                {"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": null}
              ]
            }
            """;

    @Override
    public int getLessonNumber() {
        return 11;
    }

    @Override
    public String getTitle() {
        return "Avro & Schema Registry - Production Serialization";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers Apache Avro with Confluent Schema Registry:

            1. WHY AVRO + SCHEMA REGISTRY:
               - Compact binary format (smaller than JSON)
               - Schema evolution with compatibility checks
               - Schema stored once, referenced by ID
               - Type safety and validation

            2. SCHEMA REGISTRY:
               - Central schema storage (http://localhost:8081)
               - Compatibility enforcement
               - Schema versioning
               - Integration with Kafka UI

            3. GENERICRECORD VS SPECIFICRECORD:
               - GenericRecord: Dynamic, no code generation
               - SpecificRecord: Type-safe, requires code generation

            4. COMPATIBILITY MODES:
               - BACKWARD: New schema can read old data
               - FORWARD: Old schema can read new data
               - FULL: Both directions
               - NONE: No compatibility checking

            Schema Registry UI: http://localhost:8081
            Kafka UI (with schemas): http://localhost:8080
            """;
    }

    @Override
    public void run() throws Exception {
        explainAvroAndSchemaRegistry();

        if (!isSchemaRegistryAvailable()) {
            error("Schema Registry not available at " + SCHEMA_REGISTRY_URL);
            tip("Make sure Docker Compose is running: docker compose up -d");
            tip("Wait for Schema Registry to be healthy before running this lesson.");
            return;
        }

        success("Schema Registry is available at " + SCHEMA_REGISTRY_URL);

        demonstrateGenericRecord();
        demonstrateSchemaEvolution();
        demonstrateEnumsAndComplexTypes();
        explainCompatibilityRules();

        tip("View registered schemas at: http://localhost:8081/subjects");
        tip("Kafka UI also shows schema information for Avro topics!");
    }

    private boolean isSchemaRegistryAvailable() {
        try {
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
                new java.net.URL(SCHEMA_REGISTRY_URL + "/subjects").openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(2000);
            int responseCode = conn.getResponseCode();
            return responseCode == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private void explainAvroAndSchemaRegistry() {
        explain("1. WHY AVRO + SCHEMA REGISTRY?");

        System.out.println("""

            JSON vs Avro Comparison:
            ┌─────────────────────────────────────────────────────────────────────────┐
            │                        JSON                      AVRO                   │
            ├─────────────────────────────────────────────────────────────────────────┤
            │ Format              Text (human-readable)       Binary (compact)        │
            │ Schema              Implicit in data            Explicit, stored once   │
            │ Size                Larger (field names)        Smaller (no field names)│
            │ Evolution           Manual compatibility        Automatic checking      │
            │ Validation          Runtime only                Compile + Runtime       │
            │ Speed               Slower parsing              Faster serialization    │
            └─────────────────────────────────────────────────────────────────────────┘

            Schema Registry Architecture:
            ┌─────────────────────────────────────────────────────────────────────────┐
            │                                                                         │
            │  Producer                                                               │
            │     │                                                                   │
            │     │ 1. Register schema (first time)                                   │
            │     │    or get schema ID (subsequent)                                  │
            │     ▼                                                                   │
            │  ┌─────────────────────┐                                                │
            │  │  Schema Registry    │  Stores: subject -> [schema versions]          │
            │  │  (localhost:8081)   │  Returns: schema ID (int32)                    │
            │  └─────────────────────┘                                                │
            │     │                                                                   │
            │     │ 2. Schema ID                                                      │
            │     ▼                                                                   │
            │  Producer serializes: [magic byte][schema ID (4 bytes)][avro data]      │
            │     │                                                                   │
            │     │ 3. Send to Kafka                                                  │
            │     ▼                                                                   │
            │  ┌─────────────────────┐                                                │
            │  │      Kafka          │  Message contains schema ID, not full schema   │
            │  └─────────────────────┘                                                │
            │     │                                                                   │
            │     │ 4. Consumer reads                                                 │
            │     ▼                                                                   │
            │  Consumer fetches schema by ID from Registry, then deserializes         │
            │                                                                         │
            └─────────────────────────────────────────────────────────────────────────┘

            Benefits:
            • Schema stored once, messages contain only 5-byte overhead
            • Consumers auto-fetch correct schema version
            • Breaking changes blocked by compatibility rules
            """);
    }

    private void demonstrateGenericRecord() throws Exception {
        explain("2. GENERICRECORD: Dynamic Avro without code generation");

        step("Creating Avro producer with Schema Registry...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);

        System.out.println("""

            Producer Configuration for Avro:
            ┌─────────────────────────────────────────────────────────────┐
            │ value.serializer = KafkaAvroSerializer                      │
            │ schema.registry.url = http://localhost:8081                 │
            │ auto.register.schemas = true (dev only!)                    │
            └─────────────────────────────────────────────────────────────┘
            """);

        // Parse the schema
        Schema orderSchema = new Schema.Parser().parse(ORDER_SCHEMA_V1);

        step("Creating GenericRecord from schema...");

        // Create a GenericRecord
        GenericRecord order = new GenericData.Record(orderSchema);
        order.put("orderId", "ORD-" + UUID.randomUUID().toString().substring(0, 8));
        order.put("customerId", "CUST-001");
        order.put("product", "Kafka Learning Guide");
        order.put("quantity", 2);
        order.put("totalPrice", 99.98);
        order.put("status", "CONFIRMED");
        order.put("timestamp", System.currentTimeMillis());

        result("GenericRecord created:");
        result("  " + order);

        step("Sending Avro message to Kafka...");

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, GenericRecord> record =
                new ProducerRecord<>(TOPIC + "-orders", order.get("orderId").toString(), order);

            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();

            success("Avro message sent!");
            result("Topic: " + metadata.topic());
            result("Partition: " + metadata.partition());
            result("Offset: " + metadata.offset());
            result("Serialized size: " + metadata.serializedValueSize() + " bytes");
        }

        // Show schema registration
        step("Schema automatically registered in Schema Registry");
        result("Subject: " + TOPIC + "-orders-value");
        tip("View at: http://localhost:8081/subjects/" + TOPIC + "-orders-value/versions");
    }

    private void demonstrateSchemaEvolution() throws Exception {
        explain("3. SCHEMA EVOLUTION: Adding fields safely");

        step("Sending message with V1 schema (original)...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);

        Schema schemaV1 = new Schema.Parser().parse(ORDER_SCHEMA_V1);
        GenericRecord orderV1 = new GenericData.Record(schemaV1);
        orderV1.put("orderId", "ORD-V1-001");
        orderV1.put("customerId", "CUST-001");
        orderV1.put("product", "Widget");
        orderV1.put("quantity", 1);
        orderV1.put("totalPrice", 29.99);
        orderV1.put("status", "PENDING");
        orderV1.put("timestamp", System.currentTimeMillis());

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(TOPIC + "-evolution", "v1-order", orderV1)).get();
            success("V1 message sent (7 fields)");
        }

        step("Evolving schema to V2 (adding optional shippingAddress)...");

        System.out.println("""

            Schema Evolution - Adding Optional Field:
            ┌─────────────────────────────────────────────────────────────────────────┐
            │                                                                         │
            │  V1 Schema                      V2 Schema                               │
            │  ─────────                      ─────────                               │
            │  orderId: string                orderId: string                         │
            │  customerId: string             customerId: string                      │
            │  product: string                product: string                         │
            │  quantity: int                  quantity: int                           │
            │  totalPrice: double             totalPrice: double                      │
            │  status: string                 status: string                          │
            │  timestamp: long                timestamp: long                         │
            │                                 shippingAddress: string? (NEW, optional)│
            │                                                                         │
            │  BACKWARD COMPATIBLE: V2 consumers can read V1 data                     │
            │  (missing field gets default value: null)                               │
            │                                                                         │
            └─────────────────────────────────────────────────────────────────────────┘
            """);

        Schema schemaV2 = new Schema.Parser().parse(ORDER_SCHEMA_V2);
        GenericRecord orderV2 = new GenericData.Record(schemaV2);
        orderV2.put("orderId", "ORD-V2-001");
        orderV2.put("customerId", "CUST-002");
        orderV2.put("product", "Gadget");
        orderV2.put("quantity", 3);
        orderV2.put("totalPrice", 149.97);
        orderV2.put("status", "CONFIRMED");
        orderV2.put("timestamp", System.currentTimeMillis());
        orderV2.put("shippingAddress", "123 Main St, City, Country");

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(TOPIC + "-evolution", "v2-order", orderV2)).get();
            success("V2 message sent (8 fields, includes shippingAddress)");
        }

        result("Both V1 and V2 messages coexist on the same topic!");
        result("Consumers with V2 schema can read both versions");
    }

    private void demonstrateEnumsAndComplexTypes() throws Exception {
        explain("4. COMPLEX TYPES: Enums, Maps, and Unions");

        step("Creating UserEvent with enum and map types...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);

        Schema userEventSchema = new Schema.Parser().parse(USER_EVENT_SCHEMA);

        System.out.println("""

            Complex Avro Types:
            ┌─────────────────────────────────────────────────────────────────────────┐
            │                                                                         │
            │  ENUM: Finite set of values                                             │
            │    {"type": "enum", "name": "EventType",                                │
            │     "symbols": ["CREATED", "UPDATED", "DELETED", "LOGIN"]}              │
            │                                                                         │
            │  UNION: Multiple possible types (nullable fields)                       │
            │    ["null", "string"]  -> Optional string                               │
            │    ["null", {"type": "map", "values": "string"}] -> Optional map        │
            │                                                                         │
            │  MAP: Key-value pairs (keys always strings)                             │
            │    {"type": "map", "values": "string"}                                  │
            │                                                                         │
            │  ARRAY: List of items                                                   │
            │    {"type": "array", "items": "string"}                                 │
            │                                                                         │
            └─────────────────────────────────────────────────────────────────────────┘
            """);

        // Create UserEvent with enum
        GenericRecord userEvent = new GenericData.Record(userEventSchema);
        userEvent.put("userId", "USER-" + UUID.randomUUID().toString().substring(0, 8));
        userEvent.put("email", "user@example.com");

        // Create enum value
        Schema enumSchema = userEventSchema.getField("eventType").schema();
        GenericData.EnumSymbol eventType = new GenericData.EnumSymbol(enumSchema, "LOGIN");
        userEvent.put("eventType", eventType);

        userEvent.put("timestamp", System.currentTimeMillis());

        // Create metadata map
        Map<String, String> metadata = new HashMap<>();
        metadata.put("ip", "192.168.1.100");
        metadata.put("userAgent", "Mozilla/5.0");
        metadata.put("sessionId", UUID.randomUUID().toString());
        userEvent.put("metadata", metadata);

        result("UserEvent created with:");
        result("  Enum eventType: " + userEvent.get("eventType"));
        result("  Map metadata: " + userEvent.get("metadata"));

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(TOPIC + "-users",
                userEvent.get("userId").toString(), userEvent)).get();
            success("UserEvent with complex types sent!");
        }
    }

    private void explainCompatibilityRules() {
        explain("5. COMPATIBILITY RULES: Preventing breaking changes");

        System.out.println("""

            Schema Compatibility Modes:
            ┌─────────────────────────────────────────────────────────────────────────┐
            │                                                                         │
            │  BACKWARD (default)                                                     │
            │  ─────────────────                                                      │
            │  New schema can read data written with old schema                       │
            │  Allowed: Add optional fields, remove fields                            │
            │  Use when: Consumers upgrade before producers                           │
            │                                                                         │
            │  FORWARD                                                                │
            │  ───────                                                                │
            │  Old schema can read data written with new schema                       │
            │  Allowed: Add fields, remove optional fields                            │
            │  Use when: Producers upgrade before consumers                           │
            │                                                                         │
            │  FULL                                                                   │
            │  ────                                                                   │
            │  Both backward and forward compatible                                   │
            │  Allowed: Add/remove optional fields only                               │
            │  Use when: Independent consumer/producer upgrades                       │
            │                                                                         │
            │  NONE                                                                   │
            │  ────                                                                   │
            │  No compatibility checking                                              │
            │  Use when: Development only (not recommended for production)            │
            │                                                                         │
            └─────────────────────────────────────────────────────────────────────────┘

            Compatibility Rules Quick Reference:
            ┌─────────────────────────────────────────────────────────────────────────┐
            │ Change                    │ BACKWARD │ FORWARD │ FULL │ NONE          │
            ├─────────────────────────────────────────────────────────────────────────┤
            │ Add optional field        │    ✓     │    ✓    │  ✓   │  ✓            │
            │ Add required field        │    ✗     │    ✓    │  ✗   │  ✓            │
            │ Remove optional field     │    ✓     │    ✓    │  ✓   │  ✓            │
            │ Remove required field     │    ✓     │    ✗    │  ✗   │  ✓            │
            │ Rename field              │    ✗     │    ✗    │  ✗   │  ✓            │
            │ Change field type         │    ✗     │    ✗    │  ✗   │  ✓            │
            │ Add enum symbol           │    ✗     │    ✓    │  ✗   │  ✓            │
            │ Remove enum symbol        │    ✓     │    ✗    │  ✗   │  ✓            │
            └─────────────────────────────────────────────────────────────────────────┘

            Set compatibility via REST API:
            ┌─────────────────────────────────────────────────────────────────────────┐
            │ PUT http://localhost:8081/config/my-topic-value                         │
            │ Content-Type: application/json                                          │
            │ {"compatibility": "FULL"}                                               │
            └─────────────────────────────────────────────────────────────────────────┘
            """);

        tip("Start with BACKWARD compatibility for most use cases.");
        tip("Use FULL compatibility when consumers and producers upgrade independently.");
        tip("Never use NONE in production!");
    }
}
