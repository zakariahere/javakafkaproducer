# Kafka Producer Learning Agent

> Master Apache Kafka Producer patterns from basics to production-ready configurations through hands-on, runnable lessons.

```
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║           KAFKA PRODUCER LEARNING AGENT                              ║
║           =============================                              ║
║                                                                      ║
║   11 Progressive Lessons • Spring Boot 3.5 • Java 25                 ║
║   Zero Config Setup • Interactive CLI • Visual Explanations         ║
║   Includes Schema Registry + Avro!                                   ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
```

## Why This Project?

Learning Kafka can be overwhelming. Documentation is vast, configurations are many, and it's hard to know what matters. This project takes a different approach:

- **Learn by doing** - Every concept has runnable code
- **Progressive complexity** - Start simple, build up to production patterns
- **Visual explanations** - ASCII diagrams explain what's happening
- **Zero setup friction** - Spring Boot auto-starts Kafka via Docker
- **See your messages** - Kafka UI included for visual verification
- **Production-ready** - Includes Avro + Schema Registry (Lesson 11)

## Prerequisites

**Option A: Docker Only (easiest)**
- **Docker Desktop** running
- That's it!

**Option B: Local Development**
- **Java 21+** (25 recommended)
- **Docker Desktop** running
- **Maven** (wrapper included)

---

## Quick Start (Docker Only - No Java Required!)

The fastest way to start learning - just Docker, no Java installation needed:

```bash
# Download the standalone compose file
curl -O https://raw.githubusercontent.com/zakariahere/javakafkaproducer/refs/heads/master/docker-compose.standalone.yaml

# Start everything (Kafka + Schema Registry + Kafka UI + Learning Agent)
docker compose -f docker-compose.standalone.yaml up

# Or run in detached mode and attach to the learning agent
docker compose -f docker-compose.standalone.yaml up -d
docker attach kafka-learning-agent
```

The interactive lesson menu will appear in your terminal. Open http://localhost:8080 to see your messages in Kafka UI!

**To stop:**
```bash
docker compose -f docker-compose.standalone.yaml down
```

---

## Quick Start (Local Development)

For contributors or those who want to modify the code:

```bash
# Clone the repo
git clone https://github.com/zakariahere/javakafkaproducer.git
cd kafka-producer-learning-agent

# Run (Spring Boot auto-starts Kafka + Schema Registry + Kafka UI via Docker Compose)
./mvnw spring-boot:run    # Linux/Mac
mvnw.cmd spring-boot:run  # Windows
```

You'll see an interactive menu:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AVAILABLE LESSONS                            │
├─────────────────────────────────────────────────────────────────────┤
│   1. Producer Basics - Send Patterns                                │
│   2. Serialization - String, JSON, and Custom                       │
│   3. Partitioning - Keys and Distribution                           │
│   4. Callbacks & Futures - Handling Results                         │
│   5. Error Handling - Retries and Recovery                          │
│   6. Transactions - Exactly-Once Semantics                          │
│   7. Idempotency - Preventing Duplicates                            │
│   8. Batching & Compression - Throughput Tuning                     │
│   9. Interceptors - Logging, Metrics, Transformation                │
│  10. Performance - Production Tuning & Benchmarking                 │
│  11. Avro & Schema Registry - Production Serialization              │
├─────────────────────────────────────────────────────────────────────┤
│  Module 1 (Lessons 1-3):   Foundations                              │
│  Module 2 (Lessons 4-6):   Reliability                              │
│  Module 3 (Lessons 7-9):   Advanced Patterns                        │
│  Module 4 (Lessons 10-11): Production Readiness                     │
└─────────────────────────────────────────────────────────────────────┘

Enter lesson number (1-11), 'a' for all, or 'q' to quit:
```

**View your messages:**
- Kafka UI: http://localhost:8080
- Schema Registry: http://localhost:8081

## Curriculum

### Module 1: Foundations

| Lesson | Topic | What You'll Learn |
|--------|-------|-------------------|
| **01** | Basics | Fire-and-forget, synchronous, and async send patterns |
| **02** | Serialization | String, JSON, and manual serialization strategies |
| **03** | Partitioning | Key-based routing, custom partitioners, ordering guarantees |

### Module 2: Reliability

| Lesson | Topic | What You'll Learn |
|--------|-------|-------------------|
| **04** | Callbacks | CompletableFuture, RecordMetadata, batch tracking |
| **05** | Error Handling | Retries, backoff, dead letter topics |
| **06** | Transactions | Exactly-once semantics, atomic multi-topic writes |

### Module 3: Advanced Patterns

| Lesson | Topic | What You'll Learn |
|--------|-------|-------------------|
| **07** | Idempotency | Producer IDs, sequence numbers, duplicate prevention |
| **08** | Batching | batch.size, linger.ms, compression algorithms |
| **09** | Interceptors | ProducerInterceptor for logging, metrics, headers |

### Module 4: Production Readiness

| Lesson | Topic | What You'll Learn |
|--------|-------|-------------------|
| **10** | Performance | Tuning guide, benchmarking, production checklist |
| **11** | Avro & Schema Registry | Binary serialization, schema evolution, compatibility rules |

## Recommended Learning Path

```
Week 1: Foundations
├── Day 1-2: Lesson 01 (Basics) - Understand the three send patterns
├── Day 3-4: Lesson 02 (Serialization) - Master JSON serialization
└── Day 5:   Lesson 03 (Partitioning) - Learn key-based routing

Week 2: Reliability
├── Day 1-2: Lesson 04 (Callbacks) - Handle async results properly
├── Day 3-4: Lesson 05 (Error Handling) - Build resilient producers
└── Day 5:   Lesson 06 (Transactions) - Achieve exactly-once delivery

Week 3: Advanced & Production
├── Day 1:   Lesson 07 (Idempotency) - Prevent duplicates
├── Day 2-3: Lesson 08 (Batching) - Optimize throughput
├── Day 4:   Lesson 09 (Interceptors) - Add cross-cutting concerns
├── Day 5:   Lesson 10 (Performance) - Production-ready tuning
└── Bonus:   Lesson 11 (Avro) - Schema Registry for the real world
```

## Project Structure

```
kafkaproducer/
├── compose.yaml                    # Infrastructure only (for local dev)
├── docker-compose.standalone.yaml  # Complete stack (for Docker-only users)
├── Dockerfile                      # Multi-stage build for the app
├── src/main/avro/                  # Avro schema definitions (.avsc)
│   ├── OrderEvent.avsc
│   └── UserEvent.avsc
├── src/main/java/com/elzakaria/kafkaproducer/
│   ├── KafkaproducerApplication.java
│   ├── config/
│   │   └── KafkaConfig.java        # Shared Kafka configuration
│   ├── model/
│   │   ├── Order.java              # Sample domain model
│   │   └── User.java               # Sample domain model
│   └── lessons/
│       ├── Lesson.java             # Base interface
│       ├── LessonRunner.java       # Interactive CLI
│       ├── lesson01_basics/
│       ├── lesson02_serialization/
│       ├── lesson03_partitioning/
│       ├── lesson04_callbacks/
│       ├── lesson05_error_handling/
│       ├── lesson06_transactions/
│       ├── lesson07_idempotency/
│       ├── lesson08_batching/
│       ├── lesson09_interceptors/
│       ├── lesson10_performance/
│       └── lesson11_avro/          # Avro + Schema Registry
└── src/main/resources/
    └── application.properties
```

## Docker Services

### Standalone Mode (docker-compose.standalone.yaml)
Complete learning environment - everything in Docker:

| Service | Port | Purpose |
|---------|------|---------|
| **kafka** | 9092 | Message broker (KRaft mode, no Zookeeper) |
| **schema-registry** | 8081 | Avro schema storage and validation |
| **kafka-ui** | 8080 | Visual message and schema browser |
| **learning-agent** | - | Interactive CLI lessons (attaches to terminal) |

### Development Mode (compose.yaml)
Infrastructure only - run the app locally with Java:

| Service | Port | Purpose |
|---------|------|---------|
| **kafka** | 9092 | Message broker (KRaft mode, no Zookeeper) |
| **schema-registry** | 8081 | Avro schema storage and validation |
| **kafka-ui** | 8080 | Visual message and schema browser |

## Key Configurations Explained

The lessons teach these critical producer configurations:

```properties
# Reliability
acks=all                    # Wait for all replicas (safest)
enable.idempotence=true     # Prevent duplicates from retries
retries=3                   # Retry transient failures

# Performance
batch.size=32768            # 32KB batches
linger.ms=5                 # Wait 5ms to fill batches
compression.type=lz4        # Fast compression

# Memory
buffer.memory=33554432      # 32MB buffer for unsent messages
max.block.ms=60000          # Block 60s when buffer full

# Avro / Schema Registry
schema.registry.url=http://localhost:8081
value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
auto.register.schemas=true  # Dev only! Disable in production
```

## Example Output

Here's what Lesson 11 (Avro) looks like:

```
======================================================================
LESSON 11: Avro & Schema Registry - Production Serialization
======================================================================

>> 1. WHY AVRO + SCHEMA REGISTRY?

    JSON vs Avro Comparison:
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                        JSON                      AVRO                   │
    ├─────────────────────────────────────────────────────────────────────────┤
    │ Format              Text (human-readable)       Binary (compact)        │
    │ Schema              Implicit in data            Explicit, stored once   │
    │ Size                Larger (field names)        Smaller (no field names)│
    │ Evolution           Manual compatibility        Automatic checking      │
    └─────────────────────────────────────────────────────────────────────────┘

[STEP] Sending Avro message to Kafka...
  [OK] Avro message sent!
  -> Topic: lesson11-avro-orders
  -> Partition: 0
  -> Offset: 0
  -> Serialized size: 45 bytes

[TIP] View registered schemas at: http://localhost:8081/subjects
```

## Tech Stack

- **Spring Boot 3.5.10** - Latest Spring Boot with Docker Compose support
- **Spring Kafka** - Spring's Kafka integration
- **Java 25** - Latest Java features (records, text blocks)
- **Apache Kafka** - KRaft mode (no Zookeeper needed)
- **Confluent Schema Registry** - Schema storage and evolution
- **Apache Avro** - Binary serialization format
- **Kafka UI** - Visual message and schema browser
- **Lombok** - Reduced boilerplate
- **Docker Compose** - Zero-config local infrastructure

## Commands Reference

### Running Locally (with Java)
```bash
# Run the application (starts Kafka + Schema Registry automatically)
./mvnw spring-boot:run

# Run tests
./mvnw test

# Build JAR
./mvnw clean package
```

### Running with Docker (no Java required)
```bash
# Start the complete learning environment
docker compose -f docker-compose.standalone.yaml up

# Run in background, then attach
docker compose -f docker-compose.standalone.yaml up -d
docker attach kafka-learning-agent

# Stop everything
docker compose -f docker-compose.standalone.yaml down

# Stop and remove volumes (clean slate)
docker compose -f docker-compose.standalone.yaml down -v
```

### Building the Docker Image (for maintainers)
```bash
# Build locally
docker build -t kafka-producer-learning-agent:latest .

# Build and run locally
docker compose -f docker-compose.standalone.yaml up --build

# Tag and push to registry
docker tag kafka-producer-learning-agent:latest ghcr.io/elzakaria/kafka-producer-learning-agent:latest
docker push ghcr.io/elzakaria/kafka-producer-learning-agent:latest
```

### Schema Registry Commands
```bash
# View registered schemas
curl http://localhost:8081/subjects

# View schema versions
curl http://localhost:8081/subjects/lesson11-avro-orders-value/versions

# Get specific schema
curl http://localhost:8081/subjects/lesson11-avro-orders-value/versions/1
```

## Troubleshooting

### Docker not starting?
Make sure Docker Desktop is running before starting the application.

### Port 9092 already in use?
Stop any existing Kafka instances:
```bash
docker ps | grep kafka
docker stop <container_id>
```

### Port 8080 or 8081 already in use?
Modify `compose.yaml` to use different ports:
```yaml
kafka-ui:
  ports:
    - "8082:8080"  # Change Kafka UI to 8082
schema-registry:
  ports:
    - "8083:8081"  # Change Schema Registry to 8083
```

### Schema Registry not ready for Lesson 11?
Schema Registry takes a few seconds to start. If Lesson 11 fails:
1. Wait 10-15 seconds after application starts
2. Verify Schema Registry is healthy: `curl http://localhost:8081/subjects`
3. Re-run Lesson 11

### Messages not appearing in Kafka UI?
1. Wait a few seconds for Kafka to fully start
2. Refresh the Kafka UI page
3. Check the Topics section - topics are auto-created

## Contributing

Contributions are welcome! Ideas for improvement:

- [x] ~~Add Schema Registry + Avro lesson~~ (Done! Lesson 11)
- [x] ~~Docker image for easy distribution~~ (Done! docker-compose.standalone.yaml)
- [ ] Add consumer lessons (companion project?)
- [ ] Add Kotlin version
- [ ] Add integration tests for lessons
- [ ] Add Spring Cloud Stream comparison
- [ ] Add Protobuf serialization lesson
- [ ] GitHub Actions for auto-building Docker image

## License

MIT License - Feel free to use this for learning, teaching, or building upon.

---

## About

Created to make Kafka learning accessible and hands-on. If this helped you, consider:
- ⭐ Starring the repo
- 🐛 Reporting issues
- 🔀 Contributing improvements
- 📢 Sharing with others learning Kafka

**Happy Learning!** 🚀

```
"The best way to learn Kafka is to produce messages and see what happens."
```
