package com.elzakaria.kafkaproducer.lessons.lesson09_interceptors;

import com.elzakaria.kafkaproducer.lessons.Lesson;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Lesson 09: Producer Interceptors
 *
 * Learn to intercept and transform messages:
 * 1. ProducerInterceptor interface
 * 2. Logging and metrics interceptors
 * 3. Message transformation
 * 4. Chaining interceptors
 */
@Component
public class Lesson09Interceptors implements Lesson {

    private static final String TOPIC = "lesson09-interceptors";

    private final Map<String, Object> baseProps;

    public Lesson09Interceptors() {
        this.baseProps = new HashMap<>();
        baseProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        baseProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        baseProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    @Override
    public int getLessonNumber() {
        return 9;
    }

    @Override
    public String getTitle() {
        return "Interceptors - Logging, Metrics, Transformation";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers ProducerInterceptor for cross-cutting concerns:

            1. INTERCEPTOR INTERFACE:
               - onSend(): Called before serialization
               - onAcknowledgement(): Called after broker response
               - close(): Cleanup resources

            2. USE CASES:
               - Logging all produced messages
               - Collecting metrics (latency, success rate)
               - Adding headers (trace IDs, timestamps)
               - Message transformation/enrichment

            3. CHAINING:
               - Multiple interceptors execute in order
               - Each sees output of previous interceptor
               - Error in one doesn't stop others

            4. BEST PRACTICES:
               - Keep interceptors fast (on critical path)
               - Handle exceptions gracefully
               - Use for cross-cutting concerns only
            """;
    }

    @Override
    public void run() throws Exception {
        explainInterceptorInterface();
        demonstrateLoggingInterceptor();
        demonstrateMetricsInterceptor();
        demonstrateChainedInterceptors();

        tip("Interceptors run on the producer thread - keep them fast!");
        tip("Use onAcknowledgement for metrics, onSend for transformation.");
    }

    private void explainInterceptorInterface() {
        explain("1. INTERCEPTOR INTERFACE: ProducerInterceptor methods");

        System.out.println("""

            ProducerInterceptor<K, V> Interface:
            ┌─────────────────────────────────────────────────────────────────┐
            │                                                                 │
            │  ProducerRecord<K,V> onSend(ProducerRecord<K,V> record)        │
            │    - Called BEFORE serialization                                │
            │    - Can modify/replace the record                              │
            │    - Return modified record or original                         │
            │                                                                 │
            │  void onAcknowledgement(RecordMetadata metadata,               │
            │                         Exception exception)                    │
            │    - Called AFTER broker response (or timeout)                  │
            │    - metadata is non-null on success                            │
            │    - exception is non-null on failure                           │
            │    - Good for metrics collection                                │
            │                                                                 │
            │  void close()                                                   │
            │    - Called when producer closes                                │
            │    - Cleanup resources (connections, files, etc.)               │
            │                                                                 │
            │  void configure(Map<String, ?> configs)                        │
            │    - Called with producer configuration                         │
            │    - Initialize interceptor state                               │
            │                                                                 │
            └─────────────────────────────────────────────────────────────────┘

            Message Flow with Interceptor:
            ┌─────────────────────────────────────────────────────────────────┐
            │                                                                 │
            │  send() -> [onSend()] -> serialize -> batch -> network -> broker│
            │                                                      │          │
            │                                        [onAcknowledgement()] <──┘
            │                                                                 │
            └─────────────────────────────────────────────────────────────────┘
            """);
    }

    private void demonstrateLoggingInterceptor() throws Exception {
        explain("2. LOGGING INTERCEPTOR: Trace all messages");

        step("Creating producer with logging interceptor...");

        Map<String, Object> props = new HashMap<>(baseProps);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingInterceptor.class.getName());

        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(props);
        KafkaTemplate<String, String> template = new KafkaTemplate<>(factory);

        step("Sending messages (watch interceptor logs)...");

        for (int i = 1; i <= 3; i++) {
            template.send(TOPIC, "key-" + i, "Message #" + i).get();
        }

        waitFor(1, "acknowledgement callbacks to complete");

        template.destroy();

        System.out.println("""

            LoggingInterceptor Implementation:
            ┌─────────────────────────────────────────────────────────────────┐
            │ public class LoggingInterceptor                                 │
            │         implements ProducerInterceptor<String, String> {        │
            │                                                                 │
            │     public ProducerRecord<String, String> onSend(              │
            │             ProducerRecord<String, String> record) {            │
            │         log.info("Sending: topic={}, key={}",                   │
            │                  record.topic(), record.key());                 │
            │         return record;  // Return unchanged                     │
            │     }                                                           │
            │                                                                 │
            │     public void onAcknowledgement(RecordMetadata m,            │
            │                                   Exception ex) {               │
            │         if (ex == null) {                                       │
            │             log.info("Delivered: partition={}, offset={}",     │
            │                      m.partition(), m.offset());                │
            │         } else {                                                │
            │             log.error("Failed: {}", ex.getMessage());          │
            │         }                                                       │
            │     }                                                           │
            │ }                                                               │
            └─────────────────────────────────────────────────────────────────┘
            """);
    }

    private void demonstrateMetricsInterceptor() throws Exception {
        explain("3. METRICS INTERCEPTOR: Track send statistics");

        step("Creating producer with metrics interceptor...");

        // Reset metrics
        MetricsInterceptor.resetMetrics();

        Map<String, Object> props = new HashMap<>(baseProps);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MetricsInterceptor.class.getName());

        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(props);
        KafkaTemplate<String, String> template = new KafkaTemplate<>(factory);

        step("Sending batch of messages...");

        for (int i = 1; i <= 20; i++) {
            template.send(TOPIC, "metrics-key-" + i, "Metrics message #" + i);
        }

        // Flush and wait
        template.flush();
        waitFor(2, "all acknowledgements");

        step("Checking collected metrics...");

        MetricsInterceptor.printMetrics();

        template.destroy();

        System.out.println("""

            MetricsInterceptor Pattern:
            ┌─────────────────────────────────────────────────────────────────┐
            │ Typical Metrics to Collect:                                     │
            │                                                                 │
            │ - messages_sent_total: Counter of all send attempts             │
            │ - messages_success_total: Counter of successful sends           │
            │ - messages_failed_total: Counter of failed sends                │
            │ - send_latency_ms: Histogram of send latencies                  │
            │ - message_size_bytes: Histogram of message sizes                │
            │ - messages_per_topic: Counter per topic                         │
            │                                                                 │
            │ Integration with monitoring systems:                            │
            │ - Micrometer -> Prometheus/Grafana                              │
            │ - Dropwizard Metrics                                            │
            │ - Custom StatsD/InfluxDB clients                                │
            └─────────────────────────────────────────────────────────────────┘
            """);
    }

    private void demonstrateChainedInterceptors() throws Exception {
        explain("4. CHAINING INTERCEPTORS: Multiple interceptors in sequence");

        step("Creating producer with multiple interceptors...");

        Map<String, Object> props = new HashMap<>(baseProps);
        // Chain: Timestamp -> Trace -> Logging
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                TimestampInterceptor.class.getName() + "," +
                        TraceIdInterceptor.class.getName() + "," +
                        LoggingInterceptor.class.getName());

        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(props);
        KafkaTemplate<String, String> template = new KafkaTemplate<>(factory);

        step("Sending message through interceptor chain...");

        template.send(TOPIC, "chained-key", "Original message content").get();

        waitFor(1, "chain to complete");

        template.destroy();

        System.out.println("""

            Interceptor Chain Execution:
            ┌─────────────────────────────────────────────────────────────────┐
            │                                                                 │
            │  Original Message                                               │
            │        │                                                        │
            │        ▼                                                        │
            │  ┌─────────────────────┐                                        │
            │  │ TimestampInterceptor│ - Adds X-Timestamp header              │
            │  └─────────┬───────────┘                                        │
            │            ▼                                                    │
            │  ┌─────────────────────┐                                        │
            │  │ TraceIdInterceptor  │ - Adds X-Trace-Id header               │
            │  └─────────┬───────────┘                                        │
            │            ▼                                                    │
            │  ┌─────────────────────┐                                        │
            │  │ LoggingInterceptor  │ - Logs message with headers            │
            │  └─────────┬───────────┘                                        │
            │            ▼                                                    │
            │  Enriched Message (with headers)                                │
            │                                                                 │
            └─────────────────────────────────────────────────────────────────┘

            Chain Behavior:
            - Interceptors execute in configured order
            - Each receives output of previous interceptor
            - Exception in one interceptor doesn't stop others
            - onAcknowledgement called in REVERSE order
            """);

        tip("Order matters! Put transformation interceptors before logging.");
        tip("Keep interceptors independent - don't rely on execution order.");
    }
}
