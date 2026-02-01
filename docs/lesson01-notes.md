# Lesson 01 -- Producer Basics: Send Patterns

## What the infrastructure is doing

Before we even look at the lesson, it's worth being precise about what's running.
The standalone compose started Kafka in **KRaft mode** -- that's Kafka Raft, the newer
architecture where Kafka manages its own metadata without ZooKeeper. A single node is
acting as both broker and controller. The compose sets these explicitly:

```yaml
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

That last one matters immediately: **topics are created on first write**. There is no
manual `kafka-topics --create` step. When Lesson 1 sends its first message to
`lesson01-basics`, Kafka creates the topic with default settings -- 1 partition,
replication factor 1.

Also worth noting: `application.properties` sets these defaults for every producer in
the app:

```properties
spring.kafka.producer.acks=all
spring.kafka.producer.retries=3
spring.kafka.producer.properties.enable.idempotence=true
spring.kafka.producer.properties.linger.ms=5
spring.kafka.producer.properties.batch.size=32768
```

These are active for the entire lesson. `acks=all` means the broker must confirm all
in-sync replicas wrote the message before ACKing. On a single-node cluster that is just
the broker itself, so it is effectively instant -- but in a real multi-broker cluster,
this is the difference between "leader got it" and "everyone got it."

---

## The internal pipeline every single `send()` goes through

Every call to `kafkaTemplate.send()` -- regardless of whether you `.get()` it or not --
travels the same path:

```
kafkaTemplate.send(topic, key, value)
        |
        v
  1. Serialize key + value      <- StringSerializer for both (configured in KafkaConfig.java)
        |
        v
  2. Partition selection         <- DefaultPartitioner: hash(key) % numPartitions
        |                           if key is null: sticky round-robin
        v
  3. RecordAccumulator buffer    <- per-(topic, partition) buffer, up to batch.size (32KB)
        |
        v  (background Sender thread, fires when batch.size hit OR linger.ms=5ms expires)
        |
  4. Network -> Broker            <- one TCP request per batch, not per message
        |
        v
  5. Broker writes to partition log
        |
        v
  6. Broker sends ACK            <- only after all in-sync replicas confirm (acks=all)
        |
        v
  7. CompletableFuture completes <- with RecordMetadata on success, exception on failure
```

The three "send patterns" in Lesson 1 are all about **what you do at step 7**.
Steps 1-6 are identical regardless.

---

## Fire-and-forget -- what actually happens (and doesn't)

```java
kafkaTemplate.send(TOPIC, "fire-forget-key", "Hello, Kafka! (fire-and-forget)");
// nothing else. The returned CompletableFuture is discarded.
```

The message is serialized, partitioned, and buffered in the RecordAccumulator. The
background Sender thread will flush it to the broker -- you just never find out the
result. If the broker is down, if serialization fails on a different message in the same
batch, if the network drops -- you will not know. The `CompletableFuture` object still
exists in memory and will eventually complete (or fail), but nothing is listening to it.

Output confirms it landed: when we look at the synchronous send right after, its offset
is **1**, meaning fire-and-forget already wrote offset **0**. So it did succeed -- we
just had no way to verify that at send time.

**When to use:** Metrics pipelines, application traces, telemetry -- anywhere losing an
occasional message does not corrupt your business logic.

---

## Synchronous send -- blocking the thread for a guarantee

```java
CompletableFuture<SendResult<String, String>> future =
        kafkaTemplate.send(TOPIC, "sync-key", "Hello, Kafka! (synchronous)");

SendResult<String, String> result = future.get();   // <- blocks here
RecordMetadata metadata = result.getRecordMetadata();
```

`.get()` parks the calling thread until the CompletableFuture completes. The output:

```
Topic: lesson01-basics
Partition: 0
Offset: 1
Timestamp: 1769891960428
```

That `Offset: 1` is the broker's confirmation -- "I wrote your message at position 1 in
partition 0's log." The timestamp is broker-side (when it was written), not the client
clock. This is a hard guarantee: if `.get()` returns without throwing, the message is
durable on disk.

Then it sends a second message with key `"user-123"`:

```java
SendResult<String, String> result2 =
        kafkaTemplate.send(TOPIC, "user-123", "Order placed for user-123").get();
// -> Same key 'user-123' will always go to partition: 0
```

It goes to partition 0 -- but that is not interesting yet because there is only one
partition. The *point* being made here is that **the key determines the partition,
deterministically**. `DefaultPartitioner` computes `hash("user-123") % numPartitions`.
With 1 partition, everything is 0. When Lesson 3 creates a topic with 4 partitions, you
will see different keys land on different partitions, and the same key always lands on
the same one. That is how Kafka guarantees ordering-per-key.

**The cost:** throughput. You are serializing work that Kafka was designed to do
concurrently. Each `.get()` blocks until the full round-trip to the broker completes. In
a latency-sensitive app at scale, this kills you. Use it for audit logs, financial
writes, anything where you truly cannot proceed without confirmation.

### What happens to RecordMetadata when `acks=0`?

The `application.properties` here sets `acks=all`, but it is worth understanding what
happens at the other extreme. With `acks=0` the broker sends no response at all. `.get()`
still returns a `RecordMetadata`, but it is hollow:

| Field           | Value with `acks=0` | Why                                          |
|-----------------|---------------------|----------------------------------------------|
| `topic()`       | Correct             | Determined client-side before any I/O        |
| `partition()`   | Correct             | Determined client-side by the partitioner    |
| `offset()`      | `-1`                | Assigned by the broker -- unknowable         |
| `timestamp()`   | `-1`                | Same reason                                  |

The partition is a real, correct `int` -- not a sentinel. The partitioner computes
`hash(key) % numPartitions` before the message ever leaves the client. It does not
require broker confirmation. Offset and timestamp are different: they only exist after
the broker writes the message to the log, so with `acks=0` they cannot be known and
default to `-1`.

This makes `acks=0` dangerous in the synchronous pattern specifically. The code *looks*
like it is guaranteeing delivery -- it blocks, it prints metadata -- but offset `-1` and
timestamp `-1` are the tell. You have no proof the message was actually written. It is
effectively fire-and-forget with extra steps.

---

## Async with callback -- the production pattern

```java
CompletableFuture<SendResult<String, String>> future =
        kafkaTemplate.send(TOPIC, "async-key", "Hello, Kafka! (async with callback)");

future.whenComplete((sendResult, exception) -> {
    if (exception == null) {
        // success path
    } else {
        // failure path
    }
});

result("Code continues executing while message is being sent...");
```

Look at the output order:

```
[STEP] Sending message with success/failure callbacks...
  -> Code continues executing while message is being sent...     <- THIS prints first

[STEP] Sending batch of messages asynchronously...
  Waiting 2s: allowing async messages to be delivered
  [OK] Async callback - Message delivered to partition 0 at offset 3   <- callback fires later
```

The callback fires **after** "Code continues executing" prints. That is the key
difference from synchronous. The thread is not blocked -- it keeps going. The Sender
thread handles the broker round-trip in the background, and when the future completes,
the callback runs (on the Sender thread, not the original calling thread -- important if
you are doing anything thread-sensitive in the callback).

Then the batch of 5:

```java
for (int i = 1; i <= 5; i++) {
    final int messageNum = i;
    kafkaTemplate.send(TOPIC, "batch-key-" + i, "Batch message #" + i)
            .whenComplete((res, ex) -> {
                if (ex == null) {
                    result("Message #" + messageNum + " delivered to partition " +
                            res.getRecordMetadata().partition());
                }
            });
}

waitFor(2, "allowing async messages to be delivered");
```

Five sends fire in rapid succession -- none block. They all buffer in the
RecordAccumulator. Because `linger.ms=5`, the Sender waits up to 5ms to accumulate
messages before flushing. So these 5 messages likely went out in **one or two batches**
over the wire, not five separate network requests. That is batching doing its job
transparently.

The `waitFor(2, ...)` is just `Thread.sleep(2000)` -- giving the callbacks time to print
before the lesson exits. In real production code you would not sleep; you would compose
the futures properly (Lesson 4 covers that with `allOf()`, `CountDownLatch`, etc.).

All 5 landed on partition 0 -- again, single partition topic. But notice each has a
**different key** (`batch-key-1` through `batch-key-5`). In a multi-partition topic, these
would scatter across partitions based on their key hashes.

---

## What to verify in Kafka UI

The messages are live at `http://localhost:8080`. Navigate to Topics ->
`lesson01-basics`. You will see 8 messages total:

| Offset | Key            | Source                        |
|--------|----------------|-------------------------------|
| 0      | fire-forget-key | Fire-and-forget              |
| 1      | sync-key       | Synchronous send              |
| 2      | user-123       | Synchronous (key demo)        |
| 3      | async-key      | Async with callback           |
| 4-8    | batch-key-1..5 | Async batch of 5              |

Each message has a key and value visible in the UI -- that is the StringSerializer at
work, everything is plain text.
