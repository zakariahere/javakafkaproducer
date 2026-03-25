package com.elzakaria.kafkaconsumer.lessons.lesson12_integration;

import com.elzakaria.kafkaconsumer.lessons.Lesson;
import org.springframework.stereotype.Component;

/**
 * Lesson 12: Real-World Scenarios - Integration & Testing
 *
 * Apply learnings to production scenarios.
 */
@Component
public class Lesson12RealWorldScenarios implements Lesson {

    private static final String GROUP_ID = "lesson12-group";

    @Override
    public int getLessonNumber() {
        return 12;
    }

    @Override
    public String getTitle() {
        return "Real-World Scenarios - Integration & Testing";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers production integration patterns:

            1. DATABASE INTEGRATION:
               - Consuming events and persisting to DB
               - Transaction management
               - Batch vs individual inserts
               - Connection pooling

            2. REST API ENRICHMENT:
               - Fetch additional data from APIs
               - Caching to avoid rate limits
               - Timeout and retry logic
               - Fallback strategies

            3. EVENT SOURCING:
               - Store all events in Kafka
               - Replay for state reconstruction
               - Audit trail
               - Debugging capability

            4. CQRS PATTERN:
               - Command topic (writes)
               - Event topic (read model)
               - Multiple consumers for different views
               - Eventual consistency

            5. TESTING STRATEGIES:
               - Unit tests with mocks
               - Integration tests with EmbeddedKafka
               - End-to-end tests with testcontainers
               - Chaos engineering (failure injection)

            6. CHAOS SCENARIOS:
               - Broker failures
               - Network partitions
               - Consumer crashes
               - Deserialization errors
               - Long processing times

            7. MONITORING IN TESTS:
               - Assert on lag
               - Assert on error count
               - Assert on latency percentiles
               - Assert on rebalance count
            """;
    }

    @Override
    public void run() throws Exception {
        demonstrateDatabaseIntegration();
        demonstrateApiEnrichment();
        demonstrateEventSourcing();
        demonstrateTestingApproach();
        demonstrateChaosScenarios();

        tip("Always test failure scenarios");
        tip("Use testcontainers for realistic test environments");
        tip("Chaos engineering finds bugs production testing misses");
    }

    private void demonstrateDatabaseIntegration() throws Exception {
        explain("1. DATABASE INTEGRATION: Persisting events to DB");

        System.out.println("""
            Pattern: Events → Kafka → Database

            Flow:
            1. Event produced to Kafka (immutable log)
            2. Consumer reads event
            3. Validates event
            4. Persists to database (mutable state)
            5. Commits offset

            Database Design:
            CREATE TABLE events (
                id BIGINT PRIMARY KEY,
                event_id VARCHAR(100) UNIQUE,  -- For deduplication
                topic VARCHAR(100),
                partition INT,
                kafka_offset BIGINT,           -- For resume
                data JSON,
                processed_at TIMESTAMP,
                created_at TIMESTAMP
            );

            CREATE TABLE aggregate_state (
                aggregate_id VARCHAR(100) PRIMARY KEY,
                version BIGINT,
                state JSON,
                updated_at TIMESTAMP
            );

            Considerations:
            ├─ Batch inserts: Insert multiple events at once (faster)
            ├─ Connection pool: HikariCP or similar
            ├─ Transaction management: Rollback on error
            ├─ Deduplication: Check event_id before insert
            ├─ Idempotency: UPSERT for updates
            └─ Monitoring: Track insert latency, failure rate
            """);

        result("Best practices:");
        result("1. Use prepared statements (prevent SQL injection)");
        result("2. Batch inserts every N messages");
        result("3. Handle connection pool exhaustion");
        result("4. Implement retry logic for transient failures");
        result("5. Monitor database query latency");

        tip("Batch inserts are 10-100x faster than individual inserts");
        tip("Use event_id for deduplication in database");
    }

    private void demonstrateApiEnrichment() throws Exception {
        explain("2. REST API ENRICHMENT: Enhancing events with external data");

        System.out.println("""
            Pattern: Events → Kafka → Fetch API → Enrich → Sink

            Example: Order event needs customer details from API

            Flow:
            1. Consume order event from Kafka
            2. Call /api/customers/{customer_id}
            3. Add customer details to event
            4. Persist enriched event
            5. Commit offset

            Challenges:
            ├─ API rate limits (100 req/sec)
            ├─ API latency (p99 = 500ms)
            ├─ API timeout/unavailable
            ├─ Large payload enrichment
            └─ Should all messages be enriched?

            Solutions:
            ├─ Caching: LocalCache with TTL
            ├─ Batching: Batch API calls (fewer requests)
            ├─ Async: Non-blocking API calls
            ├─ Fallback: Use cached value if API fails
            ├─ Skip: Don't enrich everything (configurable)
            └─ Throttle: Rate-limit API calls
            """);

        result("Example caching + fallback:");
        System.out.println("""
            Map<String, CustomerData> cache = new ConcurrentHashMap<>();
            
            CustomerData getCustomer(String customerId) {
                // Check cache
                return cache.computeIfAbsent(customerId, key -> {
                    try {
                        // Call API with timeout
                        return apiClient.getCustomer(key, timeout=1s);
                    } catch (TimeoutException | RetryableException e) {
                        // Fall back to minimal data
                        return new CustomerData(key, "Unknown", "Unknown");
                    }
                });
            }
            """);

        tip("Cache aggressively (customer data changes rarely)");
        tip("Set aggressive timeouts on API calls");
        tip("Implement circuit breaker to detect API degradation");
    }

    private void demonstrateEventSourcing() throws Exception {
        explain("3. EVENT SOURCING: Using Kafka as event store");

        System.out.println("""
            Event Sourcing Model:

            Traditional:
            ├─ Database state: account balance = $1000
            ├─ Action: withdraw $100
            └─ Update: balance = $900

            Event Sourcing:
            ├─ Events: [deposited($1000), withdrawn($100)]
            ├─ Current state: Sum of all events = $900
            ├─ Audit trail: Every action recorded
            └─ Replay: Can rebuild state at any point in time

            Advantages:
            ├─ Complete audit trail (immutable log)
            ├─ Replay for debugging
            ├─ Time travel (state at any timestamp)
            ├─ CQRS (separate read/write models)
            └─ Better testing (predictable event sequence)

            Disadvantages:
            ├─ More complex (rebuild state from events)
            ├─ Space overhead (store all history)
            ├─ Consistency delays (eventual consistency)
            └─ Schema evolution challenges

            Implementation:
            1. All writes go to event topic
            2. Projections read events and build state
            3. Multiple projections for different views
            4. Replayers can reconstruct any state
            """);

        result("Example event types:");
        System.out.println("""
            AccountOpened(accountId, customerId, initialBalance)
            MoneyDeposited(accountId, amount, timestamp)
            MoneyWithdrawn(accountId, amount, timestamp)
            InterestEarned(accountId, amount, timestamp)
            AccountClosed(accountId, closedAt)
            """);

        result("Projection (read model) example:");
        System.out.println("""
            class AccountBalance {
                String accountId;
                double balance;
                List<Transaction> history;
                
                void applyEvent(AccountEvent event) {
                    switch(event.type) {
                        case OPENED: balance = event.initialBalance; break;
                        case DEPOSITED: balance += event.amount; break;
                        case WITHDRAWN: balance -= event.amount; break;
                    }
                    history.add(transaction);
                }
            }
            """);

        tip("Kafka is perfect for event sourcing (immutable log)");
        tip("Use Kafka retention policy for audit requirements");
    }

    private void demonstrateTestingApproach() throws Exception {
        explain("4. TESTING STRATEGIES: Comprehensive test coverage");

        System.out.println("""
            Unit Tests (Mocked Kafka):
            ├─ Fast (no broker needed)
            ├─ Test business logic in isolation
            ├─ Mock KafkaConsumer
            ├─ Assert on processing results
            └─ Example: JUnit + Mockito

            Integration Tests (EmbeddedKafka):
            ├─ Real Kafka broker in memory
            ├─ Test consumer + processing logic
            ├─ Test error handling
            ├─ Assert on lag, offsets, retries
            └─ Example: Spring Kafka Test

            End-to-End Tests (Testcontainers):
            ├─ Real Kafka + Real Database
            ├─ Full pipeline: Event → Kafka → DB
            ├─ Test the entire system
            ├─ Slower but comprehensive
            └─ Example: testcontainers-java

            Test Scenarios:
            ├─ Happy path (normal message flow)
            ├─ Deserialization error (poison pill)
            ├─ Database connection failure
            ├─ Timeout on external API
            ├─ Rebalancing during processing
            ├─ Consumer crash and restart
            ├─ Broker unavailable
            └─ Slow consumer (catch-up scenario)
            """);

        result("Example Spring Kafka Test:");
        System.out.println("""
            @EmbeddedKafka
            public class ConsumerIntegrationTest {
                
                @Test
                public void testEventProcessing() throws InterruptedException {
                    // Produce test event
                    kafkaTemplate.send("events", "test-event");
                    
                    // Wait for consumption
                    assertTrue(latch.await(10, SECONDS));
                    
                    // Assert results
                    assertEquals(1, processedCount);
                    assertEquals(1000L, consumerLag());
                }
                
                @Test
                public void testDeserializationError() {
                    // Send malformed message
                    sendRawBytes("invalid json");
                    
                    // Should send to DLT, not crash
                    assertEquals(1, dltCount());
                    assertEquals("healthy", consumerHealth());
                }
            }
            """);

        tip("Use EmbeddedKafka for fast integration tests");
        tip("Use testcontainers for realistic E2E tests");
        tip("Test both success and failure paths");
    }

    private void demonstrateChaosScenarios() throws Exception {
        explain("5. CHAOS ENGINEERING: Injecting failures");

        System.out.println("""
            Failure Scenarios to Test:

            BROKER FAILURES:
            ├─ Broker restart during processing
            ├─ Broker network partition
            ├─ Broker disk full
            └─ Broker coordinator unavailable

            Consumer FAILURES:
            ├─ Consumer process crash
            ├─ Consumer GC pause > session.timeout
            ├─ Consumer blocked on I/O
            ├─ Consumer out of memory
            └─ Consumer network disconnected

            MESSAGE FAILURES:
            ├─ Poison pill (undeserializable)
            ├─ Message too large
            ├─ Message with invalid schema
            ├─ Message corruption
            └─ Duplicate message

            PROCESSING FAILURES:
            ├─ Exception in message handler
            ├─ Database connection failure
            ├─ API timeout or failure
            ├─ Insufficient permissions
            └─ Business rule violation

            TIMING FAILURES:
            ├─ Rebalancing during processing
            ├─ Slow broker response
            ├─ Consumer lag accumulation
            ├─ Cascading failures from lag
            └─ Timeout propagation

            Testing with Chaos:
            1. Inject failure
            2. Verify consumer behavior
            3. Verify recovery
            4. Measure impact metrics
            5. Fix if needed
            """);

        result("Example chaos test:");
        System.out.println("""
            @Test
            public void testConsumerRecoversFromBrokerFailure() {
                // Start consumer
                startConsumer();
                
                // Inject: Broker unavailable
                pauseBroker();
                waitFor(30);
                
                // Verify: Consumer detects issue
                assertTrue(consumerErrorRate() > 0);
                assertTrue(consumerLag() > 0);
                
                // Recover: Broker comes back
                resumeBroker();
                waitFor(10);
                
                // Verify: Consumer recovers
                assertEquals(0, consumerErrorRate());
                assertEquals(0, consumerLag());
            }
            """);

        tip("Use Docker for chaos (kill container, network delay)");
        tip("Use testcontainers for complex scenarios");
        tip("Measure: MTTR (Mean Time To Recovery)");
        tip("Goal: Consumer fully recovers in < 1 minute");
    }
}

