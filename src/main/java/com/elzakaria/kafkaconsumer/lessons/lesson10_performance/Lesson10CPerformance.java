package com.elzakaria.kafkaconsumer.lessons.lesson10_performance;

import com.elzakaria.kafkaconsumer.ConsumerUtils;
import com.elzakaria.kafkaconsumer.lessons.ConsumerLesson;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Lesson 10: Performance Tuning & Metrics
 *
 * Optimize consumer throughput and latency.
 */
@Component
public class Lesson10CPerformance implements ConsumerLesson {

    private static final String TOPIC = "lesson10-performance";
    private static final String GROUP_ID = "lesson10-group";

    @Override
    public int getLessonNumber() {
        return 10;
    }

    @Override
    public String getTitle() {
        return "Performance Tuning & Metrics";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers performance optimization:

            1. FETCH SETTINGS:
               - fetch.min.bytes: Minimum batch size before responding
                 (default: 1 byte = return immediately)
               - fetch.max.wait.ms: Max time to wait for batch
                 (default: 500ms)
               - Tradeoff: Small batch = low latency, high network overhead
                          Large batch = high latency, efficient use

            2. POLL TIMEOUT:
               - poll(Duration timeout): Max wait for messages
               - Too long: Rebalancing detection delayed
               - Too short: CPU usage high, poll() returned no messages
               - Optimal: 100-1000ms depending on requirements

            3. BATCH PROCESSING:
               - max.poll.records: Max messages per poll
               - Higher = more throughput, higher memory
               - Lower = more responsive to kills/rebalancing

            4. CONSUMER LAG:
               - Distance from end of topic
               - Monitor to detect processing slowness
               - Alert on increasing lag

            5. MEMORY PRESSURE:
               - Fetched records kept in memory
               - Large batches = high GC pressure
               - Monitor heap usage and GC pause times

            6. NETWORK EFFICIENCY:
               - Compression: gzip, snappy, lz4
               - Batch sends reduce network overhead
               - Monitoring: bytes/sec in and out

            7. KEY METRICS:
               - records-per-second: Throughput
               - fetch-latency: Time to get messages
               - commit-latency: Time for offset commits
               - lag: Messages behind
            """;
    }

    @Override
    public void run() throws Exception {
        ConsumerUtils.createTopics(2, TOPIC);

        produceMessagesForPerf();

        demonstrateLatencyVsThroughput();
        demonstrateMemoryPressure();
        demonstrateMetricsCollection();

        tip("Profile with your actual workload - no one-size-fits-all");
        tip("Monitor all three: throughput, latency, resource usage");
    }

    private void produceMessagesForPerf() throws Exception {
        explain("Producing test messages for performance testing");

        var producerProps = new HashMap<String, Object>();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps)) {
            for (int i = 1; i <= 100; i++) {
                producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                        TOPIC, "key-" + i, "Message-" + i + "-" + "x".repeat(100)
                ));
            }
            producer.flush();
            result("Produced 100 messages");
        }
    }

    private void demonstrateLatencyVsThroughput() throws Exception {
        explain("1. LATENCY vs THROUGHPUT: Fetch settings tradeoff");

        System.out.println("""
            Low-Latency Configuration:
            ├─ fetch.min.bytes = 1
            ├─ fetch.max.wait.ms = 10
            ├─ poll(100ms)
            └─ Result: Messages arrive in ~10-20ms, lower throughput

            High-Throughput Configuration:
            ├─ fetch.min.bytes = 102400 (100KB)
            ├─ fetch.max.wait.ms = 1000
            ├─ poll(1000ms)
            └─ Result: Messages batch efficiently, lower latency variance

            Optimal for Real-Time Systems:
            ├─ fetch.min.bytes = 1024 (1KB)
            ├─ fetch.max.wait.ms = 100
            ├─ poll(500ms)
            └─ Result: Balanced latency and throughput
            """);

        testConfiguration("Low-Latency", 1, 10, Duration.ofSeconds(1));
        testConfiguration("High-Throughput", 102400, 1000, Duration.ofSeconds(1));
        testConfiguration("Balanced", 1024, 100, Duration.ofSeconds(1));
    }

    private void testConfiguration(String name, int minBytes, int maxWaitMs, Duration pollTimeout) throws Exception {
        step("Testing " + name + " configuration...");

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-" + name);
        props.put("fetch.min.bytes", minBytes);
        props.put("fetch.max.wait.ms", maxWaitMs);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            long startTime = System.currentTimeMillis();
            int totalMessages = 0;
            int pollCount = 0;

            for (int i = 0; i < 5; i++) {
                long pollStart = System.currentTimeMillis();
                ConsumerRecords<String, String> records = consumer.poll(pollTimeout);

                if (!records.isEmpty()) {
                    totalMessages += records.count();
                    long pollLatency = System.currentTimeMillis() - pollStart;
                    result(name + " Poll #" + (i+1) + ": " + records.count() + " messages in " + pollLatency + "ms");
                }
                pollCount++;
            }

            long totalTime = System.currentTimeMillis() - startTime;
            double throughput = (totalMessages * 1000.0) / totalTime;

            System.out.println("    Config: " + name);
            System.out.println("      Total: " + totalMessages + " messages");
            System.out.println("      Time: " + totalTime + "ms");
            System.out.println("      Throughput: " + String.format("%.0f", throughput) + " msg/sec");

            consumer.commitSync();
        }
    }

    private void demonstrateMemoryPressure() throws Exception {
        explain("2. MEMORY PRESSURE: Heap usage and GC impact");

        System.out.println("""
            Memory Factors:
            ├─ fetch.min.bytes: Higher = larger batches in memory
            ├─ max.poll.records: More records = more memory
            ├─ Record size: Larger records = proportional memory
            └─ Unflushed batches: Waiting for processing

            Memory Management:
            ├─ Monitor: -Xmx, current heap, GC pauses
            ├─ Alert: GC pauses > 100ms (STW)
            ├─ Optimize: Reduce batch size, increase timeout
            └─ Last resort: Reduce consumer count (less parallelism)
            """);

        step("Measuring memory usage...");

        Runtime runtime = Runtime.getRuntime();
        long beforeGC = runtime.totalMemory() - runtime.freeMemory();

        Map<String, Object> props = createConsumerProps(GROUP_ID + "-memory");
        props.put("max.poll.records", 500);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            long afterFetch = runtime.totalMemory() - runtime.freeMemory();
            result("Heap before fetch: " + (beforeGC / 1024) + "KB");
            result("Heap after fetch: " + (afterFetch / 1024) + "KB");
            result("Heap increase: " + ((afterFetch - beforeGC) / 1024) + "KB");

            consumer.commitSync();
        }

        tip("Use jstack to identify GC pressure");
        tip("Monitor: jstat -gc -h20 <pid> 250");
    }

    private void demonstrateMetricsCollection() throws Exception {
        explain("3. METRICS: Monitoring consumer health");

        System.out.println("""
            Key Metrics to Collect:

            THROUGHPUT METRICS:
            ├─ records-consumed-rate: Messages/sec
            ├─ bytes-consumed-rate: Bytes/sec
            └─ records-per-second: App-level throughput

            LATENCY METRICS:
            ├─ fetch-latency-avg: Average poll latency
            ├─ fetch-latency-max: Max poll latency
            └─ commit-latency-avg: Offset commit time

            LAG METRICS:
            ├─ records-lag: Total messages behind
            ├─ records-lag-max: Worst partition
            └─ lag-trend: Is lag growing?

            REBALANCING METRICS:
            ├─ rebalance-latency-total: Time spent rebalancing
            ├─ rebalance-total: Number of rebalances
            └─ time-since-last-rebalance: Stability metric

            RESOURCE METRICS:
            ├─ thread-count: Active threads
            ├─ heap-usage: Memory consumption
            └─ gc-pause-time-ms: Pause durations

            Alert Thresholds:
            ├─ lag > 10000: Alert (falling behind)
            ├─ lag growing for 5 min: Warning
            ├─ gc-pause > 100ms: Investigate
            ├─ rebalance-count > 1/hour: Instability
            └─ fetch-latency-p99 > 10s: Network issue
            """);

        step("Example metrics export...");

        result("With Micrometer (Spring):");
        System.out.println("""
            @Component
            public class ConsumerMetrics {
                private final MeterRegistry meterRegistry;
                
                public void recordProcessing(long durationMs) {
                    Timer.record(durationMs, () -> {
                        // Processing
                    });
                }
                
                public void recordLag(long lag) {
                    meterRegistry.gauge("kafka.consumer.lag", lag);
                }
            }
            """);

        tip("Export metrics to Prometheus every 60s");
        tip("Use Grafana for visualization and alerting");
        tip("Set SLOs based on business requirements");
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

