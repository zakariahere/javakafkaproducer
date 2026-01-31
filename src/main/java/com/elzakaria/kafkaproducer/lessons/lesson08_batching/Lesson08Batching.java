package com.elzakaria.kafkaproducer.lessons.lesson08_batching;

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

/**
 * Lesson 08: Batching and Compression
 *
 * Learn to optimize producer throughput:
 * 1. How batching works
 * 2. batch.size and linger.ms tuning
 * 3. Compression options
 * 4. Performance tradeoffs
 */
@Component
public class Lesson08Batching implements Lesson {

    private static final String TOPIC = "lesson08-batching";
    private static final int MESSAGE_COUNT = 1000;

    private final Map<String, Object> baseProps;

    public Lesson08Batching() {
        this.baseProps = new HashMap<>();
        baseProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        baseProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        baseProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    @Override
    public int getLessonNumber() {
        return 8;
    }

    @Override
    public String getTitle() {
        return "Batching & Compression - Throughput Tuning";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers batching and compression for high throughput:

            1. BATCHING BASICS:
               - Producer accumulates messages into batches
               - Batch sent when full OR linger.ms expires
               - One network round-trip per batch, not per message

            2. KEY CONFIGURATIONS:
               - batch.size: Max batch size in bytes (default: 16KB)
               - linger.ms: Wait time to accumulate batch (default: 0)
               - buffer.memory: Total memory for batching (default: 32MB)

            3. COMPRESSION:
               - compression.type: none, gzip, snappy, lz4, zstd
               - Trades CPU for network bandwidth
               - Compressed at batch level for efficiency

            4. TRADEOFFS:
               - Larger batches = higher throughput, higher latency
               - More compression = less bandwidth, more CPU
            """;
    }

    @Override
    public void run() throws Exception {
        explainBatchingConcepts();
        compareNoBatchingVsBatching();
        demonstrateLingerEffect();
        compareCompressionOptions();

        tip("Start with linger.ms=5-10 and batch.size=32KB for balanced performance.");
        tip("Use lz4 or zstd for best compression/speed tradeoff.");
    }

    private void explainBatchingConcepts() {
        explain("1. BATCHING CONCEPTS: How the producer accumulates messages");

        System.out.println("""

            Message Flow Without Batching:
            ┌─────────────────────────────────────────────────────────────────┐
            │  App    Producer    Network    Broker                          │
            │   │        │           │          │                            │
            │   │─ M1 ──►│──────────────────────►│  3 round trips!           │
            │   │─ M2 ──►│──────────────────────►│                           │
            │   │─ M3 ──►│──────────────────────►│                           │
            └─────────────────────────────────────────────────────────────────┘

            Message Flow With Batching:
            ┌─────────────────────────────────────────────────────────────────┐
            │  App    Producer       Network       Broker                    │
            │   │        │              │             │                       │
            │   │─ M1 ──►│ [batch]      │             │                       │
            │   │─ M2 ──►│ [batch]      │             │                       │
            │   │─ M3 ──►│ [batch]      │             │                       │
            │   │        │─────────────────────────►│  1 round trip!          │
            │   │        │  [M1,M2,M3]              │                         │
            └─────────────────────────────────────────────────────────────────┘

            Batch Lifecycle:
            ┌─────────────────────────────────────────────────────────────────┐
            │                                                                 │
            │  New batch created per (topic, partition) combination           │
            │                                                                 │
            │  Batch is sent when:                                            │
            │    1. batch.size reached (e.g., 16KB of messages)               │
            │    2. linger.ms expires (e.g., 5ms since first message)         │
            │    3. Another batch is ready for same broker                    │
            │                                                                 │
            └─────────────────────────────────────────────────────────────────┘
            """);
    }

    private void compareNoBatchingVsBatching() throws Exception {
        explain("2. PERFORMANCE COMPARISON: No batching vs batching");

        step("Sending " + MESSAGE_COUNT + " messages WITHOUT batching (linger.ms=0)...");

        Map<String, Object> noBatchProps = new HashMap<>(baseProps);
        noBatchProps.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        noBatchProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 1); // Effectively disable batching

        long noBatchTime = sendMessages(noBatchProps, "no-batch");
        result("Time: " + noBatchTime + "ms");

        step("Sending " + MESSAGE_COUNT + " messages WITH batching (linger.ms=10, batch.size=64KB)...");

        Map<String, Object> batchProps = new HashMap<>(baseProps);
        batchProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        batchProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); // 64KB

        long batchTime = sendMessages(batchProps, "with-batch");
        result("Time: " + batchTime + "ms");

        double improvement = ((double) noBatchTime - batchTime) / noBatchTime * 100;
        success(String.format("Batching improved throughput by %.1f%%", improvement));

        System.out.println("""

            Configuration Comparison:
            ┌────────────────────┬─────────────────┬─────────────────┐
            │ Setting            │ No Batching     │ With Batching   │
            ├────────────────────┼─────────────────┼─────────────────┤
            │ linger.ms          │ 0               │ 10              │
            │ batch.size         │ 1 byte          │ 64 KB           │
            │ Network calls      │ ~1000           │ ~20-50          │
            │ Latency per msg    │ Lower           │ Higher (+10ms)  │
            │ Throughput         │ Lower           │ Higher          │
            └────────────────────┴─────────────────┴─────────────────┘
            """);
    }

    private void demonstrateLingerEffect() throws Exception {
        explain("3. LINGER.MS EFFECT: Trading latency for throughput");

        int[] lingerValues = {0, 5, 20, 50};

        System.out.println("\n    Testing different linger.ms values:\n");

        for (int linger : lingerValues) {
            Map<String, Object> props = new HashMap<>(baseProps);
            props.put(ProducerConfig.LINGER_MS_CONFIG, linger);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);

            long time = sendMessages(props, "linger-" + linger);
            System.out.printf("    linger.ms=%2d: %4dms for %d messages%n",
                    linger, time, MESSAGE_COUNT);
        }

        System.out.println("""

            Linger.ms Guidelines:
            ┌─────────────────────────────────────────────────────────────────┐
            │ linger.ms=0   : Send immediately, lowest latency, less batching│
            │ linger.ms=5   : Good balance for most applications             │
            │ linger.ms=20  : Better batching, acceptable latency            │
            │ linger.ms=100+: High throughput batch processing only          │
            └─────────────────────────────────────────────────────────────────┘
            """);
    }

    private void compareCompressionOptions() throws Exception {
        explain("4. COMPRESSION: CPU vs bandwidth tradeoffs");

        String[] compressionTypes = {"none", "gzip", "snappy", "lz4", "zstd"};

        System.out.println("\n    Comparing compression algorithms:\n");

        for (String compression : compressionTypes) {
            Map<String, Object> props = new HashMap<>(baseProps);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);

            long time = sendMessages(props, "compress-" + compression);
            System.out.printf("    %-6s: %4dms%n", compression, time);
        }

        System.out.println("""

            Compression Comparison:
            ┌─────────────┬───────────┬───────────┬─────────────────────────┐
            │ Algorithm   │ Ratio     │ CPU       │ Best For                │
            ├─────────────┼───────────┼───────────┼─────────────────────────┤
            │ none        │ 1:1       │ None      │ Already compressed data │
            │ gzip        │ Best      │ High      │ Cold storage, archival  │
            │ snappy      │ Good      │ Low       │ General purpose         │
            │ lz4         │ Good      │ Very Low  │ High throughput (rec)   │
            │ zstd        │ Very Good │ Medium    │ Best balance (rec)      │
            └─────────────┴───────────┴───────────┴─────────────────────────┘

            Recommended:
            - lz4: Best for maximum throughput
            - zstd: Best compression/speed tradeoff (Kafka 2.1+)
            - snappy: Good all-around, widely used
            """);

        tip("Compression happens at batch level - bigger batches compress better.");
        tip("Consumer decompresses automatically, no config needed.");
    }

    private long sendMessages(Map<String, Object> props, String prefix) throws Exception {
        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(props);
        KafkaTemplate<String, String> template = new KafkaTemplate<>(factory);

        List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            futures.add(template.send(TOPIC, prefix + "-" + i, generatePayload(i)));
        }

        // Wait for all to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

        long duration = System.currentTimeMillis() - startTime;

        template.destroy();
        return duration;
    }

    private String generatePayload(int index) {
        // Generate a reasonably sized payload (~200 bytes)
        return String.format(
                "{\"id\":%d,\"timestamp\":%d,\"data\":\"Message payload for testing batching and compression performance in Kafka producer lesson eight\"}",
                index,
                System.currentTimeMillis()
        );
    }
}
