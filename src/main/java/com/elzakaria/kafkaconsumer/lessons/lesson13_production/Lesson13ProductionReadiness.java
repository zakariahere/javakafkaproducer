package com.elzakaria.kafkaconsumer.lessons.lesson13_production;

import com.elzakaria.kafkaconsumer.ConsumerUtils;
import com.elzakaria.kafkaconsumer.lessons.ConsumerLesson;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Lesson 13: Production Readiness - Monitoring & Operations
 *
 * Deploy consumers in production safely.
 */
@Component
public class Lesson13ProductionReadiness implements ConsumerLesson {

    public static final String TOPIC = "lesson13-lag";
    private static final String GROUP_ID = "lesson13-group";

    @Override
    public int getLessonNumber() {
        return 13;
    }

    @Override
    public String getTitle() {
        return "Production Readiness - Monitoring & Operations";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers production deployment and operations:

            1. CONSUMER HEALTH CHECKS:
               - Lag monitoring: Is consumer falling behind?
               - Rebalance frequency: Is consumer stable?
               - Error rate: How many messages are failing?
               - Processing latency: How fast are we?

            2. MONITORING INFRASTRUCTURE:
               - Metrics collection: Prometheus, Grafana
               - Log aggregation: ELK, Loki
               - Alerting: Alert rules and escalation
               - Dashboards: Real-time visibility

            3. GRACEFUL SHUTDOWN:
               - Stop accepting new work
               - Wait for in-flight processing
               - Commit final offsets
               - Close connections cleanly

            4. CONSUMER SCALING:
               - Horizontal: Add more consumers to group
               - Partitions must >= consumers (else some idle)
               - Max consumers = partition count
               - Rebalancing when adding/removing

            5. DEAD LETTER TOPIC MANAGEMENT:
               - Monitoring DLT size
               - Analyzing failures
               - Potential replay after fixes
               - Preventing DLT explosion

            6. OPERATIONAL RUNBOOKS:
               - Consumer lag too high
               - Consumer stuck on poison pill
               - Broker partition leadership
               - Consumer group reset procedures

            7. CAPACITY PLANNING:
               - Messages per second
               - Message size
               - Required consumers = messages/sec / (throughput per consumer)
               - Reserve 20-30% for headroom
            """;
    }

    @Override
    public void run() throws Exception {
        ConsumerUtils.createTopics(1, TOPIC);

        demonstrateHealthCheck();
        demonstrateLagMonitoring();
        demonstrateRebalanceMetrics();
        demonstrateOperationalTasks();
        demonstrateScalingStrategy();

        tip("Implement automated health checks");
        tip("Monitor lag above all else");
        tip("Plan capacity based on message volume, not partition count");
    }

    private void demonstrateHealthCheck() throws Exception {
        explain("1. HEALTH CHECKS: Verify consumer is functioning");

        System.out.println("""
            Health Check Components:

            LIVENESS (Is it alive?):
            ├─ Last poll timestamp < 1 minute
            └─ Consumer not crashed/hung

            READINESS (Can it process?):
            ├─ No rebalancing in progress
            ├─ Lag < threshold
            └─ Error rate < threshold

            STARTUP (Did it initialize?):
            ├─ Consumer group exists
            ├─ At least one partition assigned
            └─ Able to poll messages

            Endpoints:
            GET /health/live     → 200 if alive, 503 if not
            GET /health/ready    → 200 if ready, 503 if not
            GET /health/startup  → 200 if started, 503 if not
            """);

        step("Implementing health check logic...");

        result("Pseudo-code for liveness check:");
        System.out.println("""
            boolean isConsumerAlive() {
                long lastPollTime = getCurrentTime();
                return (getCurrentTime() - lastPollTime) < Duration.ofMinutes(1);
            }
            """);

        result("Pseudo-code for readiness check:");
        System.out.println("""
            boolean isConsumerReady() {
                return !isRebalancing() &&
                       getConsumerLag() < LAG_THRESHOLD &&
                       getErrorRate() < ERROR_THRESHOLD;
            }
            """);

        success("Health checks enable infrastructure monitoring");

        tip("Spring Boot Actuator provides /actuator/health endpoints");
        tip("Kubernetes uses these endpoints for liveness/readiness probes");
    }

    private void demonstrateLagMonitoring() throws Exception {
        explain("2. LAG MONITORING: Track processing position");

        System.out.println("""
            Consumer Lag Definition:
            lag = endOffset - committedOffset

            For each partition:
            ├─ endOffset: Position of last message
            ├─ committedOffset: What consumer processed
            └─ lag: Messages not yet consumed

            Total Lag:
            └─ Sum of lag across all partitions

            Lag Trend:
            ├─ Stable lag: Processing = production rate
            ├─ Growing lag: Falling behind (scale up!)
            ├─ Shrinking lag: Catching up (temporary)
            └─ Zero lag: Fully current

            Alert Thresholds:
            ├─ lag > 10000: Immediate alert
            ├─ lag growing for 5 min: Escalate
            ├─ lag > daily max: Investigate
            └─ lag spike: Check for errors
            """);

        step("Calculating and monitoring lag...");

        String groupId = GROUP_ID + "-lag-demo";

        // Simulate consumer
        Map<String, Object> props = createConsumerProps(groupId);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            result("Consumer monitoring lag for: " + TOPIC);
            result("Consumer group: " + groupId);

            result("Lag metrics available:");
            result("  - kafka.consumer.lag");
            result("  - kafka.consumer.lag.max");
            result("  - kafka.consumer.records.lag.avg");

            tip("Export metrics every 60 seconds");
            tip("Use time-series DB to detect trends");
            tip("Set up alerts for lag anomalies");
        } catch (Exception e) {
            result("Topic may not exist - that's ok for demo");
        }
    }

    private void demonstrateRebalanceMetrics() throws Exception {
        explain("3. REBALANCE METRICS: Monitor group stability");

        System.out.println("""
            Rebalance Metrics:

            FREQUENCY:
            ├─ rebalances-total: Cumulative count
            ├─ time-since-last-rebalance-ms: Health indicator
            └─ Alert: > 1 rebalance/hour indicates instability

            DURATION:
            ├─ rebalance-latency-total-ms: Time spent in rebalance
            ├─ rebalance-latency-max-ms: Worst rebalance
            └─ Alert: > 30 sec per rebalance (investigate)

            STABILITY SIGNALS:
            ├─ Frequent rebalances → consumer crashes/hangs
            ├─ Long rebalances → large state or many partitions
            ├─ Rebalancing + lag growth → processing too slow
            └─ Rebalancing + errors → see error logs

            Root Causes of Instability:
            ├─ GC pauses > session.timeout.ms
            ├─ Slow message processing (> max.poll.interval.ms)
            ├─ Network issues (broker unresponsive)
            ├─ Consumer resource exhaustion
            └─ Overloaded brokers
            """);

        tip("Monitor rebalance rate continuously");
        tip("Stable consumer = zero rebalances for hours");
        tip("Investigate any rebalance immediately");
    }

    private void demonstrateOperationalTasks() throws Exception {
        explain("4. OPERATIONAL RUNBOOKS: Handling common issues");

        System.out.println("""
            Issue: Consumer Lag Growing

            Investigation:
            1. Check broker health (are they responding?)
            2. Check network (latency/packet loss?)
            3. Check consumer logs (errors?)
            4. Monitor CPU/memory (resource exhausted?)
            5. Check processing latency (too slow?)

            Resolution:
            - If broker: Wait for recovery or failover
            - If network: Check topology, DNS, routing
            - If errors: See error logs, may need restart
            - If resources: Scale up consumer or add consumers
            - If slow: Optimize processing, increase parallelism

            ---

            Issue: Consumer Stuck on Poison Pill

            Symptoms:
            - Lag not growing (not reading)
            - Errors in logs (deserialization?)
            - No messages being processed

            Resolution:
            - Check error logs for problematic offset
            - Use seekToOffset to skip poison pill
            - Enable Dead Letter Topic
            - Resume processing

            Code:
            consumer.seek(new TopicPartition(topic, partition), offset + 1);

            ---

            Issue: Consumer Group Has No Leader

            Symptoms:
            - Group shows "No active members"
            - Messages not being consumed
            - Rebalancing errors

            Resolution:
            - Check consumer process (alive?)
            - Check network connectivity
            - Restart consumer
            - Check broker logs

            Prevent:
            - Increase session.timeout.ms if GC is bad
            - Add more replicas for fault tolerance
            - Monitor broker metrics
            """);

        tip("Create runbook for each common issue");
        tip("Include detection, investigation, resolution steps");
        tip("Automate where possible (auto-scaling, auto-recovery)");
    }

    private void demonstrateScalingStrategy() throws Exception {
        explain("5. SCALING STRATEGY: Handle growing volume");

        System.out.println("""
            Capacity Planning:

            FORMULA:
            required_consumers = messages_per_sec / throughput_per_consumer

            Example:
            ├─ Production rate: 100,000 msg/sec
            ├─ Consumer throughput: 10,000 msg/sec (measured)
            ├─ Required: 100,000 / 10,000 = 10 consumers
            ├─ Topic partitions: >= 10 (must be >= consumer count)
            └─ Reserved capacity: 10 * 1.3 = 13 consumers/partitions

            HORIZONTAL SCALING:
            1. Increase partition count (if < consumer count)
            2. Add consumer instances (up to partition count)
            3. Rebalancing happens automatically
            4. New consumers start consuming

            CONSTRAINTS:
            ├─ Can't decrease partition count (immutable)
            ├─ Max consumers = partition count
            ├─ Each rebalance stops processing briefly
            └─ Consumer affinity lost after rebalance

            MONITORING DURING SCALE:
            ├─ Watch rebalance progress
            ├─ Verify lag decreases after scale
            ├─ Check for errors during assignment
            ├─ Monitor new consumer memory usage
            └─ Verify throughput improved
            """);

        step("Scaling decision matrix...");

        System.out.println("""
            Metric              | Action
            ────────────────────┼──────────────────────────
            Lag < 1000          | No action
            Lag 1000-10000      | Monitor, prepare to scale
            Lag 10000-100000    | Scale soon (1-2 days)
            Lag > 100000        | Emergency scale
            Lag growing daily   | Trending scale needed
            """);

        tip("Measure throughput with actual workload");
        tip("Plan for 20-30% headroom for spike capacity");
        tip("Consider geographic distribution");
        tip("Monitor scaling cost vs performance");
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

