package com.elzakaria.kafkaproducer.lessons.lesson10_performance;

import com.elzakaria.kafkaproducer.lessons.Lesson;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Lesson 10: Production Performance Tuning
 *
 * Master Kafka producer performance optimization:
 * 1. Key configuration parameters
 * 2. Memory management
 * 3. Acknowledgment strategies
 * 4. Benchmarking methodology
 */
@Component
public class Lesson10Performance implements Lesson {

    private static final String TOPIC = "lesson10-performance";
    private static final int BENCHMARK_MESSAGES = 10000;

    private final Map<String, Object> baseProps;

    public Lesson10Performance() {
        this.baseProps = new HashMap<>();
        baseProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        baseProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        baseProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    @Override
    public int getLessonNumber() {
        return 10;
    }

    @Override
    public String getTitle() {
        return "Performance - Production Tuning & Benchmarking";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers production-ready performance tuning:

            1. KEY CONFIGURATIONS:
               - acks: Durability vs performance tradeoff
               - buffer.memory: Total memory for buffering
               - max.block.ms: How long send() blocks when buffer full

            2. MEMORY MANAGEMENT:
               - Record accumulator sizing
               - Handling backpressure
               - Memory vs latency tradeoffs

            3. ACKNOWLEDGMENT STRATEGIES:
               - acks=0: Fire and forget
               - acks=1: Leader only
               - acks=all: Full replication

            4. BENCHMARKING:
               - Throughput measurement
               - Latency percentiles
               - Resource utilization
            """;
    }

    @Override
    public void run() throws Exception {
        explainKeyConfigurations();
        demonstrateAckStrategies();
        demonstrateBufferManagement();
        runBenchmark();
        provideProductionChecklist();

        tip("Always benchmark with production-like data and load!");
        tip("Monitor broker metrics alongside producer metrics.");
    }

    private void explainKeyConfigurations() {
        explain("1. KEY CONFIGURATIONS: The most impactful settings");

        System.out.println("""

            Producer Performance Parameters:
            ┌─────────────────────────────────────────────────────────────────────────┐
            │ THROUGHPUT CONFIGURATIONS                                               │
            ├─────────────────────────────────────────────────────────────────────────┤
            │ batch.size = 16384 (16KB)                                               │
            │   - Larger batches = fewer requests = higher throughput                 │
            │   - Too large = higher latency, memory pressure                         │
            │   - Recommended: 32KB - 128KB for high throughput                       │
            │                                                                         │
            │ linger.ms = 0                                                           │
            │   - Time to wait for batch to fill                                      │
            │   - 0 = send immediately, 5-20 = good batching                          │
            │   - Higher = more batching, higher latency                              │
            │                                                                         │
            │ compression.type = none                                                 │
            │   - lz4/zstd recommended for throughput                                 │
            │   - Trades CPU for network bandwidth                                    │
            ├─────────────────────────────────────────────────────────────────────────┤
            │ MEMORY CONFIGURATIONS                                                   │
            ├─────────────────────────────────────────────────────────────────────────┤
            │ buffer.memory = 33554432 (32MB)                                         │
            │   - Total memory for unsent messages                                    │
            │   - Increase for high-throughput producers                              │
            │   - size >= batch.size * partitions * 2                                 │
            │                                                                         │
            │ max.block.ms = 60000 (60s)                                              │
            │   - How long send() blocks when buffer full                             │
            │   - Lower = fail fast, Higher = handle bursts                           │
            ├─────────────────────────────────────────────────────────────────────────┤
            │ RELIABILITY CONFIGURATIONS                                              │
            ├─────────────────────────────────────────────────────────────────────────┤
            │ acks = all                                                              │
            │   - 0: no ack, 1: leader only, all: full ISR                            │
            │   - acks=all with min.insync.replicas=2 for durability                  │
            │                                                                         │
            │ retries = 2147483647 (default)                                          │
            │   - Combined with delivery.timeout.ms for retry budget                  │
            │                                                                         │
            │ enable.idempotence = true                                               │
            │   - Prevents duplicates from retries                                    │
            │   - Default in Kafka 3.0+                                               │
            └─────────────────────────────────────────────────────────────────────────┘
            """);
    }

    private void demonstrateAckStrategies() throws Exception {
        explain("2. ACKNOWLEDGMENT STRATEGIES: Durability vs performance");

        String[] ackModes = {"0", "1", "all"};
        String[] modeDescriptions = {
                "No ack (fire-and-forget)",
                "Leader only",
                "All in-sync replicas"
        };

        System.out.println("\n    Benchmarking different acks settings (" + BENCHMARK_MESSAGES + " messages):\n");

        for (int i = 0; i < ackModes.length; i++) {
            Map<String, Object> props = new HashMap<>(baseProps);
            props.put(ProducerConfig.ACKS_CONFIG, ackModes[i]);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);

            BenchmarkResult result = runBenchmarkWithConfig(props, "acks-" + ackModes[i]);

            System.out.printf("    acks=%-3s (%s)%n", ackModes[i], modeDescriptions[i]);
            System.out.printf("      Throughput: %,.0f msgs/sec%n", result.messagesPerSecond);
            System.out.printf("      Duration: %d ms%n", result.durationMs);
            System.out.println();
        }

        System.out.println("""

            Acks Comparison:
            ┌─────────────────────────────────────────────────────────────────────────┐
            │ acks=0  │ No durability   │ Highest throughput │ Data loss possible    │
            │ acks=1  │ Leader durable  │ Good throughput    │ Loss if leader fails  │
            │ acks=all│ Full durability │ Lower throughput   │ No data loss          │
            └─────────────────────────────────────────────────────────────────────────┘

            Recommendation: Use acks=all for most production workloads.
            """);
    }

    private void demonstrateBufferManagement() throws Exception {
        explain("3. BUFFER MANAGEMENT: Handling backpressure");

        step("Demonstrating buffer behavior under load...");

        Map<String, Object> smallBufferProps = new HashMap<>(baseProps);
        smallBufferProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1048576); // 1MB
        smallBufferProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000); // 1 second
        smallBufferProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        smallBufferProps.put(ProducerConfig.LINGER_MS_CONFIG, 100); // Slow sending

        System.out.println("""

            Buffer Configuration (constrained):
            ┌─────────────────────────────────────────────┐
            │ buffer.memory = 1MB (small)                 │
            │ max.block.ms = 1000 (1 second)              │
            │ batch.size = 64KB                           │
            │ linger.ms = 100 (slow flush)                │
            └─────────────────────────────────────────────┘
            """);

        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(smallBufferProps);
        KafkaTemplate<String, String> template = new KafkaTemplate<>(factory);

        step("Sending messages rapidly to small buffer...");

        int sent = 0;
        int blocked = 0;
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 500; i++) {
            try {
                // Large message to fill buffer faster
                String largePayload = "X".repeat(10000);
                template.send(TOPIC, "buffer-test-" + i, largePayload);
                sent++;
            } catch (Exception e) {
                if (e.getMessage() != null && e.getMessage().contains("block")) {
                    blocked++;
                }
            }
        }

        template.flush();
        long duration = System.currentTimeMillis() - startTime;

        result("Sent: " + sent + ", Blocked/Failed: " + blocked);
        result("Duration: " + duration + "ms");

        template.destroy();

        System.out.println("""

            Buffer Management Best Practices:
            ┌─────────────────────────────────────────────────────────────────────────┐
            │ 1. Size buffer.memory based on:                                         │
            │    - Expected message rate                                              │
            │    - Message size                                                       │
            │    - Number of partitions                                               │
            │    - Network latency to broker                                          │
            │                                                                         │
            │ 2. Handle BufferExhaustedException:                                     │
            │    - Implement backpressure in application                              │
            │    - Use rate limiting                                                  │
            │    - Monitor buffer utilization                                         │
            │                                                                         │
            │ 3. Formula for buffer sizing:                                           │
            │    buffer.memory >= batch.size * partitions * 2                         │
            │                   + overhead for in-flight requests                     │
            └─────────────────────────────────────────────────────────────────────────┘
            """);
    }

    private void runBenchmark() throws Exception {
        explain("4. BENCHMARKING: Measuring real performance");

        step("Running comprehensive benchmark with optimized settings...");

        Map<String, Object> optimizedProps = new HashMap<>(baseProps);
        optimizedProps.put(ProducerConfig.ACKS_CONFIG, "all");
        optimizedProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        optimizedProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); // 64KB
        optimizedProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        optimizedProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        optimizedProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64MB

        System.out.println("""

            Optimized Configuration:
            ┌─────────────────────────────────────────────┐
            │ acks = all                                  │
            │ enable.idempotence = true                   │
            │ batch.size = 64KB                           │
            │ linger.ms = 10                              │
            │ compression.type = lz4                      │
            │ buffer.memory = 64MB                        │
            └─────────────────────────────────────────────┘
            """);

        BenchmarkResult result = runBenchmarkWithConfig(optimizedProps, "optimized");

        System.out.println("\n    Benchmark Results:");
        System.out.println("    ┌─────────────────────────────────────────────────┐");
        System.out.printf("    │ Messages sent:        %,10d              │%n", BENCHMARK_MESSAGES);
        System.out.printf("    │ Duration:             %,10d ms           │%n", result.durationMs);
        System.out.printf("    │ Throughput:           %,10.0f msgs/sec    │%n", result.messagesPerSecond);
        System.out.printf("    │ MB/sec:               %,10.2f             │%n", result.mbPerSecond);
        System.out.printf("    │ Avg latency:          %,10.2f ms          │%n", result.avgLatencyMs);
        System.out.println("    └─────────────────────────────────────────────────┘");

        tip("Real benchmarks should run longer and measure p99 latency.");
    }

    private void provideProductionChecklist() {
        explain("5. PRODUCTION CHECKLIST: Pre-launch verification");

        System.out.println("""

            Production Readiness Checklist:
            ┌─────────────────────────────────────────────────────────────────────────┐
            │ RELIABILITY                                                             │
            │ [ ] acks=all for critical data                                          │
            │ [ ] enable.idempotence=true                                             │
            │ [ ] retries configured with delivery.timeout.ms                         │
            │ [ ] Error handling for all send operations                              │
            │ [ ] Dead letter topic for failed messages                               │
            ├─────────────────────────────────────────────────────────────────────────┤
            │ PERFORMANCE                                                             │
            │ [ ] batch.size tuned for message size                                   │
            │ [ ] linger.ms set (5-20ms for most cases)                               │
            │ [ ] Compression enabled (lz4 or zstd)                                   │
            │ [ ] buffer.memory sized for load                                        │
            ├─────────────────────────────────────────────────────────────────────────┤
            │ MONITORING                                                              │
            │ [ ] Metrics exported (record-send-rate, buffer-available-bytes)         │
            │ [ ] Alerts on error rates                                               │
            │ [ ] Alerts on buffer exhaustion                                         │
            │ [ ] Log aggregation for troubleshooting                                 │
            ├─────────────────────────────────────────────────────────────────────────┤
            │ OPERATIONS                                                              │
            │ [ ] client.id set for identification                                    │
            │ [ ] Graceful shutdown implemented                                       │
            │ [ ] Connection pooling if using multiple templates                      │
            │ [ ] Load tested at 2x expected peak                                     │
            └─────────────────────────────────────────────────────────────────────────┘

            Key Metrics to Monitor:
            ┌─────────────────────────────────────────────────────────────────────────┐
            │ Producer Metrics (JMX):                                                 │
            │ - record-send-rate: Messages/second                                     │
            │ - record-error-rate: Failed messages/second                             │
            │ - request-latency-avg: Average request time                             │
            │ - buffer-available-bytes: Remaining buffer                              │
            │ - batch-size-avg: Average batch size                                    │
            │ - compression-rate-avg: Compression efficiency                          │
            │                                                                         │
            │ Broker Metrics:                                                         │
            │ - MessagesInPerSec: Incoming message rate                               │
            │ - BytesInPerSec: Incoming bytes                                         │
            │ - UnderReplicatedPartitions: Replication health                         │
            │ - RequestHandlerAvgIdlePercent: Broker capacity                         │
            └─────────────────────────────────────────────────────────────────────────┘
            """);
    }

    private BenchmarkResult runBenchmarkWithConfig(Map<String, Object> props, String name)
            throws Exception {

        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(props);
        KafkaTemplate<String, String> template = new KafkaTemplate<>(factory);

        List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>();
        String payload = generatePayload();
        int payloadSize = payload.getBytes().length;

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < BENCHMARK_MESSAGES; i++) {
            futures.add(template.send(TOPIC, name + "-" + i, payload));
        }

        // Wait for all completions
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(60, TimeUnit.SECONDS);

        long duration = System.currentTimeMillis() - startTime;

        template.destroy();

        double messagesPerSecond = (double) BENCHMARK_MESSAGES / duration * 1000;
        double mbPerSecond = (double) BENCHMARK_MESSAGES * payloadSize / duration / 1000; // MB/s
        double avgLatencyMs = (double) duration / BENCHMARK_MESSAGES;

        return new BenchmarkResult(duration, messagesPerSecond, mbPerSecond, avgLatencyMs);
    }

    private String generatePayload() {
        // ~500 byte message
        return String.format(
                "{\"timestamp\":%d,\"data\":\"%s\",\"metadata\":{\"source\":\"benchmark\",\"version\":\"1.0\"}}",
                System.currentTimeMillis(),
                "X".repeat(400)
        );
    }

    private record BenchmarkResult(long durationMs, double messagesPerSecond,
                                   double mbPerSecond, double avgLatencyMs) {
    }
}
