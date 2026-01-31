package com.elzakaria.kafkaproducer.lessons.lesson07_idempotency;

import com.elzakaria.kafkaproducer.lessons.Lesson;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Lesson 07: Idempotent Producer
 *
 * Learn how to prevent duplicate messages:
 * 1. The duplicate problem
 * 2. Producer idempotence
 * 3. Sequence numbers and producer IDs
 * 4. Configuration requirements
 */
@Component
public class Lesson07Idempotency implements Lesson {

    private static final String TOPIC = "lesson07-idempotent";

    private final Map<String, Object> baseProps;

    public Lesson07Idempotency() {
        this.baseProps = new HashMap<>();
        baseProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        baseProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        baseProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    @Override
    public int getLessonNumber() {
        return 7;
    }

    @Override
    public String getTitle() {
        return "Idempotency - Preventing Duplicates";
    }

    @Override
    public String getDescription() {
        return """
            This lesson explains idempotent producers:

            1. THE DUPLICATE PROBLEM:
               - Network timeout -> retry -> duplicate message
               - Broker received but ACK was lost

            2. IDEMPOTENT PRODUCER:
               - enable.idempotence=true
               - Producer ID + sequence number per partition
               - Broker deduplicates based on sequence

            3. REQUIREMENTS:
               - acks=all (or -1)
               - max.in.flight.requests.per.connection <= 5
               - retries > 0

            4. LIMITATIONS:
               - Only within single producer session
               - New producer instance = new producer ID
               - For cross-session guarantees, use transactions

            Default in Kafka 3.0+: Idempotence enabled by default!
            """;
    }

    @Override
    public void run() throws Exception {
        explainDuplicateProblem();
        demonstrateIdempotentProducer();
        demonstrateSequenceNumbers();
        explainLimitations();

        tip("In Kafka 3.0+, idempotence is enabled by default.");
        tip("For cross-session idempotence, use transactional producers.");
    }

    private void explainDuplicateProblem() {
        explain("1. THE DUPLICATE PROBLEM: How duplicates occur");

        System.out.println("""

            Without Idempotence - Duplicate Scenario:
            ┌─────────────────────────────────────────────────────────────────┐
            │                                                                 │
            │  Producer                    Broker                             │
            │     │                          │                                │
            │     │──── Send Message 1 ─────►│                                │
            │     │                          │ (writes to partition)          │
            │     │                          │                                │
            │     │◄─── ACK (lost!) ─────────│                                │
            │     │         ✗                │                                │
            │     │                          │                                │
            │     │   (timeout, retry)       │                                │
            │     │                          │                                │
            │     │──── Send Message 1 ─────►│                                │
            │     │         (retry)          │ (writes AGAIN!)                │
            │     │                          │                                │
            │     │◄─── ACK ─────────────────│                                │
            │     │                          │                                │
            │                                                                 │
            │  Result: Message 1 written TWICE to the partition!              │
            │                                                                 │
            └─────────────────────────────────────────────────────────────────┘
            """);

        result("Problem: Lost ACK + retry = duplicate message");
    }

    private void demonstrateIdempotentProducer() throws Exception {
        explain("2. IDEMPOTENT PRODUCER: Automatic deduplication");

        step("Creating idempotent producer...");

        Map<String, Object> idempotentProps = new HashMap<>(baseProps);
        idempotentProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        idempotentProps.put(ProducerConfig.ACKS_CONFIG, "all");
        idempotentProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        idempotentProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        System.out.println("""

            Idempotent Producer Configuration:
            ┌─────────────────────────────────────────────────────────────┐
            │ enable.idempotence = true                                   │
            │   - Assigns unique Producer ID (PID)                        │
            │   - Tracks sequence numbers per partition                   │
            │                                                             │
            │ acks = all                                                  │
            │   - Required for idempotence                                │
            │                                                             │
            │ max.in.flight.requests.per.connection = 5                   │
            │   - Max allowed for idempotence (preserves ordering)        │
            │                                                             │
            │ retries = 3+                                                │
            │   - Retries are safe with idempotence                       │
            └─────────────────────────────────────────────────────────────┘
            """);

        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(idempotentProps);
        KafkaTemplate<String, String> idempotentTemplate = new KafkaTemplate<>(factory);

        step("Sending messages with idempotent producer...");

        for (int i = 1; i <= 5; i++) {
            SendResult<String, String> result = idempotentTemplate.send(
                    TOPIC,
                    "key-" + i,
                    "Idempotent message #" + i
            ).get();

            result("Message #" + i + " sent to partition " +
                    result.getRecordMetadata().partition() +
                    " at offset " + result.getRecordMetadata().offset());
        }

        success("All messages sent with idempotence enabled");
        result("If any retry occurred, broker would detect and deduplicate");

        idempotentTemplate.destroy();
    }

    private void demonstrateSequenceNumbers() throws Exception {
        explain("3. SEQUENCE NUMBERS: How deduplication works");

        System.out.println("""

            With Idempotence - Deduplication:
            ┌─────────────────────────────────────────────────────────────────┐
            │                                                                 │
            │  Producer (PID=1001)           Broker                           │
            │     │                            │                              │
            │     │── Msg (PID=1001, Seq=0) ──►│                              │
            │     │                            │ Records: PID=1001, Seq=0     │
            │     │                            │ Writes message               │
            │     │◄── ACK (lost!) ────────────│                              │
            │     │         ✗                  │                              │
            │     │                            │                              │
            │     │   (timeout, retry)         │                              │
            │     │                            │                              │
            │     │── Msg (PID=1001, Seq=0) ──►│                              │
            │     │         (retry)            │ Checks: PID=1001, Seq=0      │
            │     │                            │ DUPLICATE DETECTED!          │
            │     │                            │ Returns existing offset      │
            │     │◄── ACK ────────────────────│                              │
            │     │                            │                              │
            │                                                                 │
            │  Result: Message written only ONCE!                             │
            │                                                                 │
            └─────────────────────────────────────────────────────────────────┘

            Broker maintains per-partition:
            ┌──────────────────────────────────────────────────────┐
            │  Producer ID  │  Last 5 Sequence Numbers            │
            ├──────────────────────────────────────────────────────┤
            │  PID=1001     │  [0, 1, 2, 3, 4]                     │
            │  PID=1002     │  [0, 1, 2]                           │
            └──────────────────────────────────────────────────────┘
            """);

        step("Demonstrating sequence number tracking...");

        Map<String, Object> props = new HashMap<>(baseProps);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(props);
        KafkaTemplate<String, String> template = new KafkaTemplate<>(factory);

        // Send to same partition (same key) to show sequence progression
        String fixedKey = "sequence-demo-" + UUID.randomUUID();

        System.out.println("\n    Sending 5 messages with same key (same partition):");

        for (int i = 0; i < 5; i++) {
            SendResult<String, String> result = template.send(
                    TOPIC,
                    fixedKey,
                    "Sequence message #" + i
            ).get();

            System.out.printf("    Seq %d -> Partition %d, Offset %d%n",
                    i,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
        }

        result("Broker tracks sequence 0-4 for this producer ID and partition");
        result("Any retry with same PID+Seq combination is deduplicated");

        template.destroy();
    }

    private void explainLimitations() {
        explain("4. LIMITATIONS: Understanding idempotence scope");

        System.out.println("""

            Idempotence Limitations:
            ┌─────────────────────────────────────────────────────────────────┐
            │                                                                 │
            │  1. SINGLE SESSION ONLY                                         │
            │     - New producer instance = new Producer ID                   │
            │     - Restart produces different PID                            │
            │     - No cross-session deduplication                            │
            │                                                                 │
            │  2. NO APPLICATION-LEVEL IDEMPOTENCE                            │
            │     - Doesn't prevent business logic duplicates                 │
            │     - Two sends from app = two messages (different sequences)   │
            │                                                                 │
            │  3. BROKER MEMORY BOUNDED                                       │
            │     - Broker keeps only last 5 sequences per PID/partition      │
            │     - Very old retries might not be deduplicated                │
            │                                                                 │
            └─────────────────────────────────────────────────────────────────┘

            Solutions for Cross-Session Guarantees:
            ┌─────────────────────────────────────────────────────────────────┐
            │                                                                 │
            │  1. TRANSACTIONAL PRODUCER                                      │
            │     - transactional.id survives restarts                        │
            │     - Broker fences old producers with same txn.id              │
            │                                                                 │
            │  2. APPLICATION-LEVEL DEDUPLICATION                             │
            │     - Include idempotency key in message                        │
            │     - Consumer deduplicates based on key                        │
            │     - e.g., orderId + timestamp hash                            │
            │                                                                 │
            │  3. UPSERT TO DATABASE                                          │
            │     - Consumer uses UPSERT instead of INSERT                    │
            │     - Natural deduplication based on primary key                │
            │                                                                 │
            └─────────────────────────────────────────────────────────────────┘
            """);

        tip("For critical data, combine idempotent producer + application-level keys");
        tip("Transactional producer provides strongest guarantees but adds latency");
    }
}
