# Kafka Learning Agent - Producer & Consumer

> Master Apache Kafka both as a producer and consumer through hands-on, runnable lessons covering all patterns from basics to production-ready configurations.

```
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║           KAFKA LEARNING AGENT                                       ║
║           =====================                                      ║
║                                                                      ║
║   11 Producer Lessons + 14 Consumer Lessons                          ║
║   Spring Boot 3.5 • Java 25 • Zero Config Setup                      ║
║   Interactive CLI • Visual Explanations • Unified Launcher           ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
```

## Why This Project?

Learning Kafka can be overwhelming. Documentation is vast, configurations are many, and it's hard to know what matters. This project takes a different approach:

- **Learn by doing** - Every concept has runnable code
- **Comprehensive** - Both producer AND consumer patterns in one place
- **Progressive complexity** - Start simple, build up to production patterns
- **Visual explanations** - ASCII diagrams explain what's happening
- **Zero setup friction** - Spring Boot auto-starts Kafka via Docker
- **See your messages** - Kafka UI included for visual verification
- **Production-ready** - Includes error handling, transactions, exactly-once semantics

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

You'll see an interactive menu to choose your learning path:

```
╔═════════════════════════════════════════════════════════╗
║  🎓 KAFKA LEARNING AGENT - LESSON SELECTOR 🎓          ║
╠═════════════════════════════════════════════════════════╣
│                                                         │
│  [1] 📤 KAFKA PRODUCER LESSONS (11 Lessons)            │
│      Learn to send messages with confidence             │
│      Topics: Basics, Partitioning, Serialization,      │
│              Callbacks, Transactions, Batching, Avro    │
│                                                         │
│  [2] 📥 KAFKA CONSUMER LESSONS (12 Lessons)            │
│      Master consuming messages reliably                 │
│      Topics: Polling, Groups, Offsets,                 │
│              Deserialization, Error Handling, State     │
│              Management, Concurrency, Production        │
│                                                         │
│  [q] Quit the application                              │
│                                                         │
╚═════════════════════════════════════════════════════════╝

Enter your choice (1=Producer, 2=Consumer, 'q'=Quit):
```

Choose **Option 1** for Producer lessons or **Option 2** for Consumer lessons. You can switch between them anytime!

**View your messages:**
- Kafka UI: http://localhost:8080
- Schema Registry: http://localhost:8081

## Curriculum

### Producer Lessons (11 Lessons)

#### Module 1: Foundations

| Lesson | Topic | What You'll Learn |
|--------|-------|-------------------|
| **01** | Basics | Fire-and-forget, synchronous, and async send patterns |
| **02** | Serialization | String, JSON, and manual serialization strategies |
| **03** | Partitioning | Key-based routing, custom partitioners, ordering guarantees |

#### Module 2: Reliability

| Lesson | Topic | What You'll Learn |
|--------|-------|-------------------|
| **04** | Callbacks | CompletableFuture, RecordMetadata, batch tracking |
| **05** | Error Handling | Retries, backoff, dead letter topics |
| **06** | Transactions | Exactly-once semantics, atomic multi-topic writes |

#### Module 3: Advanced Patterns

| Lesson | Topic | What You'll Learn |
|--------|-------|-------------------|
| **07** | Idempotency | Producer IDs, sequence numbers, duplicate prevention |
| **08** | Batching | batch.size, linger.ms, compression algorithms |
| **09** | Interceptors | ProducerInterceptor for logging, metrics, headers |

#### Module 4: Production Readiness

| Lesson | Topic | What You'll Learn |
|--------|-------|-------------------|
| **10** | Performance | Tuning guide, benchmarking, production checklist |
| **11** | Avro & Schema Registry | Binary serialization, schema evolution, compatibility rules |

---

### Consumer Lessons (12 Lessons)

#### Module 1: Fundamentals

| Lesson | Topic | What You'll Learn |
|--------|-------|-------------------|
| **01** | Basics | Polling model, consumer groups, partition assignment, offsets |
| **02** | Groups & Rebalancing | Partition strategies, rebalancing triggers, session timeouts |
| **03** | Offset Management | Commit strategies, lag tracking, seeking (CRITICAL!) |

#### Module 2: Reliability

| Lesson | Topic | What You'll Learn |
|--------|-------|-------------------|
| **04** | Deserialization | Type safety, error handling, Dead Letter Topics |
| **05** | Error Handling | Error classification, retries, circuit breaker patterns |
| **06** | State Management | Idempotent processing, exactly-once semantics, checkpoints |

#### Module 3: Implementation

| Lesson | Topic | What You'll Learn |
|--------|-------|-------------------|
| **07** | Concurrency | Thread safety, async processing, graceful shutdown |
| **08** | Performance | Fetch tuning, latency vs throughput, memory management |
| **09** | Advanced Patterns | Multi-topic consumption, fan-out, aggregations |

#### Module 4: Production

| Lesson | Topic | What You'll Learn |
|--------|-------|-------------------|
| **10** | Production Readiness | Health checks, monitoring, scaling strategies (ESSENTIAL!) |
| **11** | Transactions | Isolation levels, exactly-once semantics, idempotent writes |
| **12** | Integration & Testing | Database integration, API enrichment, event sourcing, testing |

## Recommended Learning Path

### Complete Learning (4 Weeks - 23 Lessons)

```
Week 1: Producer Foundations
├── Day 1-2: Lesson 01 (Basics) - Understand the three send patterns
├── Day 3-4: Lesson 02 (Serialization) - Master JSON serialization
└── Day 5:   Lesson 03 (Partitioning) - Learn key-based routing

Week 2: Producer Reliability & Consumer Foundations
├── Day 1-2: Producer Lesson 04 (Callbacks) - Handle async results
├── Day 3:   Producer Lesson 05 (Error Handling) - Build resilience
├── Day 4-5: Consumer Lesson 01-02 (Basics & Groups) - Understand consumption

Week 3: Consumer Core Topics (CRITICAL!)
├── Day 1-2: Consumer Lesson 03 (Offsets) ⭐ Most important!
├── Day 3-4: Consumer Lesson 05 (Error Handling)
└── Day 5:   Consumer Lesson 10 (Production) ⭐ Operations focus

Week 4: Advanced Topics & Production
├── Day 1:   Producer Lesson 06 (Transactions)
├── Day 2:   Consumer Lesson 11 (Transactions)
├── Day 3-4: Consumer Lesson 06-07 (State & Concurrency)
├── Day 5:   Consumer Lesson 12 (Integration & Testing)
└── Bonus:   Producer Lesson 11 (Avro & Schema Registry)
```

### Express Learning (1 Week - 8 Lessons - Producer Only)

```
Day 1:   Producer Lesson 01 (Basics)
Day 2:   Producer Lesson 02 (Serialization)
Day 3:   Producer Lesson 03 (Partitioning)
Day 4:   Producer Lesson 04 (Callbacks)
Day 5:   Producer Lesson 05 (Error Handling)
Day 6-7: Producer Lesson 06 + 10 (Transactions + Performance)
```

### Consumer-Focused Learning (2 Weeks - 12 Lessons - Consumer Only)

```
Week 1:
├── Day 1-2: Lesson 01 (Basics)
├── Day 3-4: Lesson 02 (Groups & Rebalancing)
└── Day 5:   Lesson 03 (Offsets) ⭐ CRITICAL

Week 2:
├── Day 1:   Lesson 05 (Error Handling)
├── Day 2-3: Lesson 06 + 10 (State & Production)
├── Day 4:   Lesson 07 (Concurrency)
└── Day 5:   Lesson 12 (Integration)
```

## Project Structure

```
kafkaproducer/
├── compose.yaml                      # Infrastructure only (for local dev)
├── docker-compose.standalone.yaml    # Complete stack (for Docker-only users)
├── Dockerfile                        # Multi-stage build for the app
├── src/main/avro/                    # Avro schema definitions (.avsc)
│   ├── OrderEvent.avsc
│   └── UserEvent.avsc
├── src/main/java/com/elzakaria/
│   ├── kafkaproducer/
│   │   ├── KafkaproducerApplication.java
│   │   ├── ApplicationLauncher.java  # Unified lesson selector 🆕
│   │   ├── config/
│   │   │   └── KafkaConfig.java      # Shared Kafka configuration
│   │   ├── model/
│   │   │   ├── Order.java            # Sample domain model
│   │   │   └── User.java             # Sample domain model
│   │   └── lessons/
│   │       ├── Lesson.java           # Base interface
│   │       ├── LessonRunner.java     # Interactive CLI
│   │       ├── lesson01_basics/
│   │       ├── lesson02_serialization/
│   │       ├── lesson03_partitioning/
│   │       ├── lesson04_callbacks/
│   │       ├── lesson05_error_handling/
│   │       ├── lesson06_transactions/
│   │       ├── lesson07_idempotency/
│   │       ├── lesson08_batching/
│   │       ├── lesson09_interceptors/
│   │       ├── lesson10_performance/
│   │       └── lesson11_avro/        # Avro + Schema Registry
│   │
│   └── kafkaconsumer/                # 🆕 NEW: Consumer Lessons
│       ├── KafkaconsumerApplication.java
│       ├── config/
│       │   └── KafkaConsumerConfig.java
│       └── lessons/
│           ├── Lesson.java           # Base interface
│           ├── LessonRunner.java     # Interactive CLI
│           ├── lesson01_basics/      # Polling model
│           ├── lesson02_groups/      # Rebalancing
│           ├── lesson03_offsets/     # Commit strategies
│           ├── lesson04_deserialization/
│           ├── lesson05_error_handling/
│           ├── lesson06_state_management/
│           ├── lesson07_concurrency/
│           ├── lesson08_performance/
│           ├── lesson09_advanced_patterns/
│           ├── lesson10_production/
│           ├── lesson11_transactions/
│           ├── lesson12_integration/
│           ├── lesson13_filters_and_offsets/
│           └── lesson14_kafka_listener/
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
- **Spring Kafka** - Spring's Kafka integration for both producer and consumer
- **Java 25** - Latest Java features (records, text blocks)
- **Apache Kafka** - KRaft mode (no Zookeeper needed)
- **Confluent Schema Registry** - Schema storage and evolution
- **Apache Avro** - Binary serialization format (producer lessons)
- **Kafka UI** - Visual message and schema browser
- **Lombok** - Reduced boilerplate
- **Docker Compose** - Zero-config local infrastructure
- **ApplicationLauncher** - Unified menu to switch between producer/consumer lessons 🆕

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

### What's New (Latest Updates)

**🎉 Major Updates:**
- ✅ **Consumer Lessons Added!** - 12 comprehensive lessons covering polling, groups, offsets, error handling, state management, concurrency, performance, transactions, and more
- ✅ **Unified Launcher** - Interactive menu lets you choose between producer and consumer lessons without restarting
- ✅ **Faster Startup** - Optimized Docker healthchecks (40-50% faster, ~10-15 seconds vs ~24 seconds)
- ✅ **Component Scanning** - Both producer and consumer packages are properly discovered

**📚 Key Consumer Lessons:**
- **Lesson 03 (Offsets)** ⭐ - Critical for understanding commit strategies and consumer lag
- **Lesson 10 (Production)** ⭐ - Essential for monitoring, health checks, and scaling
- **Lesson 11 (Transactions)** - Exactly-once semantics and isolation levels
- **Lesson 12 (Integration)** - Database integration, testing, and real-world patterns

**📖 Documentation Added:**
- `FIXES_APPLIED.md` - Details of bean discovery fix and Docker optimization
- `LAUNCHER_GUIDE.md` - Complete guide to using the unified launcher
- `QUICK_FIX_SUMMARY.md` - Quick reference for recent changes
- `CONSUMER_LESSONS.md` - Comprehensive consumer learning guide

### Docker Compose Startup Optimization

Recent optimization reduced startup time by 40-50%:
- **Before:** ~24 seconds (Kafka ~12s, Schema Registry ~10s)
- **After:** ~10-15 seconds
- **Changes:** Faster healthcheck intervals and timeouts, added start_period

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

- [x] ~~Add Schema Registry + Avro lesson~~ (Done! Producer Lesson 11)
- [x] ~~Add Consumer lessons~~ (Done! 12 comprehensive consumer lessons)
- [x] ~~Unified launcher for lesson selection~~ (Done! ApplicationLauncher)
- [x] ~~Docker Compose optimization~~ (Done! 40-50% faster startup)
- [ ] Add Kotlin version of lessons
- [ ] Add integration tests for lessons
- [ ] Add Spring Cloud Stream comparison
- [ ] Add Protobuf serialization lesson
- [ ] GitHub Actions for auto-building Docker image
- [ ] Web UI for lesson selection (instead of CLI)
- [ ] Add consumer lesson notes documentation

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
