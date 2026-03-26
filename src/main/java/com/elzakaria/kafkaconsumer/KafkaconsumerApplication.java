package com.elzakaria.kafkaconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Kafka Consumer Lessons Application
 *
 * An interactive educational application that teaches Kafka Consumer concepts
 * through 12 comprehensive lessons with working code examples.
 *
 * Lessons covered:
 * 1.  Consumer Basics - Polling & Message Consumption
 * 2.  Consumer Groups & Rebalancing
 * 3.  Offset Management - Commit Strategies
 * 4.  Deserialization & Type Safety
 * 5.  Error Handling - Retries and Recovery
 * 6.  State & Offset Management - Consistency Patterns
 * 7.  Concurrency & Threading - Async Processing
 * 8.  Performance Tuning & Metrics
 * 9.  Advanced Patterns - Multi-Topic & Aggregations
 * 10. Production Readiness - Monitoring & Operations
 * 11. Transactions & Exactly-Once Semantics
 * 12. Real-World Scenarios - Integration & Testing
 *
 * Prerequisites:
 * - Running Kafka broker at localhost:9092
 * - Topics will be created automatically for each lesson
 *
 * Usage:
 * - Run: ./mvnw spring-boot:run
 * - Interactive menu will appear
 * - Select lesson number, 'a' for all, or 'q' to quit
 *
 * Project Structure:
 * ├── lessons/
 * │   ├── Lesson.java (base interface)
 * │   ├── LessonRunner.java (CLI menu)
 * │   └── lesson01_basics/ through lesson12_integration/
 * ├── config/
 * │   └── KafkaConsumerConfig.java
 * └── KafkaconsumerApplication.java (entry point)
 */
@SpringBootApplication
public class KafkaconsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaconsumerApplication.class, args);
    }
}

