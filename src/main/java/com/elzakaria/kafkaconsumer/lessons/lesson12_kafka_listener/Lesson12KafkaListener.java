package com.elzakaria.kafkaconsumer.lessons.lesson12_kafka_listener;

import com.elzakaria.kafkaconsumer.ConsumerUtils;
import com.elzakaria.kafkaconsumer.lessons.ConsumerLesson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Lesson 12: Spring @KafkaListener Annotation
 *
 * Learn how to use Spring's declarative @KafkaListener annotation
 * as an alternative to manual polling. Covers:
 * 1. Basic @KafkaListener - Simple message consumption
 * 2. Multiple listeners on different topics
 * 3. Message headers and metadata access
 * 4. Manual acknowledgment
 * 5. Error handling in listeners
 * 6. Listener containers and concurrency
 */
@Component
public class Lesson12KafkaListener implements ConsumerLesson {

    private static final String TOPIC_SIMPLE = "lesson12-simple";
    private static final String TOPIC_ADVANCED = "lesson12-advanced";
    private static final String GROUP_ID = "lesson12-group";
    private static final int NUM_PARTITIONS = 2;

    // Thread-safe list to capture consumed messages for verification
    private final List<String> simpleMessages = new CopyOnWriteArrayList<>();
    private final List<String> advancedMessages = new CopyOnWriteArrayList<>();
    private CountDownLatch simpleCountdownLatch;
    private CountDownLatch advancedCountdownLatch;

    // Kafka listener registry to control listener lifecycle
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    public Lesson12KafkaListener(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    }

    @Override
    public int getLessonNumber() {
        return 12;
    }

    @Override
    public String getTitle() {
        return "Spring @KafkaListener Annotation";
    }

    @Override
    public String getDescription() {
        return """
            This lesson covers Spring's declarative @KafkaListener annotation:

            1. BASIC @KafkaListener
               - Declarative alternative to manual polling with KafkaConsumer
               - Automatically handles subscription, polling, and rebalancing
               - Topics and group.id configured via annotation parameters
               - Simpler, more Spring-like approach

            2. MULTIPLE LISTENERS
               - Create separate listener methods for different topics
               - Each listener runs in its own thread/container
               - Enables parallel processing of different message streams

            3. MESSAGE METADATA
               - Access message headers via @Header annotation
               - Get topic, partition, offset information
               - Extract correlation IDs, timestamps, and custom headers

            4. MANUAL ACKNOWLEDGMENT
               - Control when offset is committed
               - Acknowledge after successful processing
               - Ensures exactly-once semantics with proper error handling

            5. ERROR HANDLING
               - Listeners can throw exceptions
               - ErrorHandler can define retry logic
               - SeekToCurrentErrorHandler restarts from failed message

            6. KEY DIFFERENCES: @KafkaListener vs KafkaConsumer
               
               @KafkaListener (Declarative - Spring)
               ├─ Simple annotation-based setup
               ├─ Automatic polling in background
               ├─ Threads managed by Spring
               ├─ Good for standard scenarios
               └─ Less control over polling details

               KafkaConsumer (Imperative - Manual)
               ├─ Full control over polling loop
               ├─ Manual thread management
               ├─ Detailed control over behavior
               ├─ Good for complex scenarios
               └─ More code required

            7. LISTENER CONTAINER PROPERTIES
               - concurrency: Number of threads to process messages
               - poll-timeout: How long to wait for messages
               - auto-startup: Whether to start listening automatically
               - topic: Topic name(s) to subscribe
               - groupId: Consumer group identifier
            """;
    }

    @Override
    public void run() throws Exception {
        explain("Spring @KafkaListener: Declarative Message Consumption");

        try {
            ConsumerUtils.createTopics(NUM_PARTITIONS, TOPIC_SIMPLE, TOPIC_ADVANCED);

            // Start listeners only when lesson runs
            startListeners();

            demonstrateSimpleListener();
            demonstrateMultipleListeners();
            demonstrateMessageMetadata();
            demonstrateManualAcknowledgment();
            demonstrateListenerLifecycleControl();

            tip("@KafkaListener is ideal for standard message processing scenarios");
            tip("Use manual KafkaConsumer for advanced control or complex polling logic");
            tip("Listeners run in background containers - Spring manages threading");
            tip("@KafkaListener receivers are not limited to simple String messages - supports POJOs");
            tip("Use lifecycle control (pause/stop) for maintenance mode, graceful shutdown, or traffic management");
        } finally {
            // Stop listeners when lesson ends
            stopListeners();
        }
    }

    /**
     * Start all @KafkaListener containers.
     * This is called when the lesson begins.
     */
    private void startListeners() throws Exception {
        explain("Starting @KafkaListener containers...");
        kafkaListenerEndpointRegistry.getListenerContainers().forEach(container -> {
            if (container.getGroupId() != null && container.getGroupId().startsWith(GROUP_ID)) {
                if (!container.isRunning()) {
                    container.start();
                }
            }
        });
        result("Listener containers started");
        
        // Wait for listeners to fully subscribe and be ready
        // This is crucial - without this delay, messages produced immediately after
        // starting might be missed if the listener hasn't fully joined the consumer group yet
        step("Waiting for listeners to fully subscribe to topics...");
        Thread.sleep(3000);
        result("Listeners are ready");
    }

    /**
     * Stop all @KafkaListener containers.
     * This is called when the lesson ends.
     */
    private void stopListeners() {
        explain("Stopping @KafkaListener containers...");
        kafkaListenerEndpointRegistry.getListenerContainers().forEach(container -> {
            if (container.getGroupId() != null && container.getGroupId().startsWith(GROUP_ID)) {
                if (container.isRunning()) {
                    container.stop();
                }
            }
        });
        result("Listener containers stopped");
    }

    private void demonstrateSimpleListener() throws Exception {
        explain("1. BASIC @KafkaListener: Simple declarative consumption");

        step("Preparing to produce test messages to '" + TOPIC_SIMPLE + "'...");

        // Reset message list and latch BEFORE producing
        simpleMessages.clear();
        simpleCountdownLatch = new CountDownLatch(5);
        
        // Small delay to ensure listener is fully ready
        Thread.sleep(500);

        step("Producing test messages to '" + TOPIC_SIMPLE + "'...");
        // Produce test messages
        produceMessages(TOPIC_SIMPLE, 5);

        step("Listener consuming messages (running in Spring background container)...");


        // Give listener time to process
        boolean completed = simpleCountdownLatch.await(10, TimeUnit.SECONDS);

        result("Listener messages received: " + simpleMessages.size());
        System.out.println("\n  Messages consumed by @KafkaListener:");
        simpleMessages.forEach(msg -> System.out.println("    ✓ " + msg));

        if (completed) {
            success("@KafkaListener successfully consumed " + simpleMessages.size() + " messages");
        } else {
            result("Note: Only " + simpleMessages.size() + " out of 5 messages were received");
        }

        tip("@KafkaListener methods are called automatically when messages arrive");
        tip("Method must have @KafkaListener annotation with topic and groupId");
        tip("Message parameter type is automatically deserialized (String, POJO, etc.)");
    }

    private void demonstrateMultipleListeners() throws Exception {
        explain("2. MULTIPLE LISTENERS: Different topics, parallel processing");

        step("Producing messages to '" + TOPIC_ADVANCED + "'...");

        // Reset and set latch BEFORE producing
        advancedMessages.clear();
        advancedCountdownLatch = new CountDownLatch(5);
        simpleMessages.clear();
        simpleCountdownLatch = new CountDownLatch(5);

        produceMessages(TOPIC_ADVANCED, 5);
        produceMessages(TOPIC_SIMPLE, 5);

        step("Multiple @KafkaListener methods consuming from different topics...");

        boolean completedAdv = advancedCountdownLatch.await(10, TimeUnit.SECONDS);
        boolean completedSimple = advancedCountdownLatch.await(10, TimeUnit.SECONDS);

        if (completedAdv) {
            success("@KafkaListener successfully consumed " + advancedMessages.size() + " advancedMessages");
        } else {
            result("Note: Only " + advancedMessages.size() + " out of 5 advancedMessages were received");
        }
        advancedMessages.forEach(msg -> System.out.println("    ✓ " + msg));

        if (completedSimple) {
            success("@KafkaListener successfully consumed " + simpleMessages.size() + " simpleMessages");
        } else {
            result("Note: Only " + simpleMessages.size() + " out of 5 simpleMessages were received");
        }

        simpleMessages.forEach(msg -> System.out.println("    ✓ " + msg));

        result("Advanced listener messages received: " + advancedMessages.size());
        System.out.println("\n  Messages consumed by advanced listener:");

        success("Multiple listeners can run in parallel consuming different topics");

        tip("Each @KafkaListener creates its own consumer group container");
        tip("Different topics can be processed concurrently without blocking");
        tip("Containers run in separate threads managed by Spring");
    }

    private void demonstrateMessageMetadata() throws Exception {
        explain("3. MESSAGE METADATA: Accessing headers, partition, offset info");

        step("Producing messages with custom headers...");

        produceMessagesWithHeaders(TOPIC_SIMPLE, 3);

        step("Listener accessing message metadata (@Header, @Payload, etc.)...");

        result("Listener can access:");
        System.out.println("    • @Payload - message body");
        System.out.println("    • @Header(KafkaHeaders.RECEIVED_TOPIC) - topic name");
        System.out.println("    • @Header(KafkaHeaders.RECEIVED_PARTITION_ID) - partition number");
        System.out.println("    • @Header(KafkaHeaders.OFFSET) - message offset");
        System.out.println("    • @Header(\"custom-header\") - custom headers");

        tip("Headers provide context about message origin and metadata");
        tip("Correlation IDs, timestamps, and routing info can be in headers");
        tip("Message metadata is useful for debugging and auditing");
    }

    private void demonstrateManualAcknowledgment() throws Exception {
        explain("4. MANUAL ACKNOWLEDGMENT: Control when offsets are committed");

        step("Understanding Acknowledgment modes...");

        System.out.println("\n  Acknowledgment Modes:");
        System.out.println("    RECORD: Acknowledge after each message");
        System.out.println("    BATCH: Acknowledge after poll batch is processed");
        System.out.println("    TIME: Acknowledge periodically by time");
        System.out.println("    MANUAL: Explicit ack(Acknowledgment ack) in listener");
        System.out.println("    MANUAL_IMMEDIATE: Same as MANUAL");

        result("In this lesson, listeners use AckMode.MANUAL for explicit control");

        step("Listener with manual acknowledgment...");

        simpleMessages.clear();
        simpleCountdownLatch = new CountDownLatch(3);
        produceMessages(TOPIC_SIMPLE, 3);

        boolean completed = simpleCountdownLatch.await(10, TimeUnit.SECONDS);

        System.out.println("\n  Processing with explicit ack(...):");
        System.out.println("    1. Receive message");
        System.out.println("    2. Process message");
        System.out.println("    3. Call ack() to commit offset");
        System.out.println("    4. If processing fails, no ack -> message redelivered");

        success("Manual acknowledgment gives explicit control over offset commits");

        tip("Use MANUAL for: Critical processing, transactions, guaranteed delivery");
        tip("Use BATCH for: High throughput scenarios, can tolerate message loss");
        tip("Always handle errors before calling ack() - failed ack causes redelivery");
    }

    private void demonstrateListenerLifecycleControl() throws Exception {
        explain("5. LISTENER LIFECYCLE CONTROL: Start, pause, and stop listeners for maintenance");

        step("Understanding listener lifecycle states...");

        System.out.println("\n  Listener Container States:");
        System.out.println("    RUNNING: Actively polling and processing messages");
        System.out.println("    PAUSED: Stopped polling but can be restarted (maintains group membership)");
        System.out.println("    STOPPED: Completely stopped (will rebalance on restart)");

        result("Use lifecycle control for: Maintenance mode, graceful shutdown, traffic management");

        // DEMONSTRATE 1: Check initial state
        step("1. Checking initial listener state...");
        var simpleContainer = kafkaListenerEndpointRegistry.getListenerContainer("lesson12-simple");
        if (simpleContainer != null) {
            System.out.println("\n  Listener 'lesson12-simple':");
            System.out.println("    ✓ Running: " + simpleContainer.isRunning());
        }

        result("Listener is actively running and processing messages");
        
        // Reset and produce messages for this demonstration
        step("Producing messages to '" + TOPIC_SIMPLE + "'...");
        simpleMessages.clear();
        simpleCountdownLatch = new CountDownLatch(4);
        produceMessages(TOPIC_SIMPLE, 4);
        
        Thread.sleep(2000); // Allow some messages to be processed

        // DEMONSTRATE 2: Pause the listener (maintenance mode)
        step("2. Pausing listener (like entering maintenance mode)...");
        if (simpleContainer != null && simpleContainer.isRunning()) {
            simpleContainer.pause();
            Thread.sleep(1000);

            System.out.println("\n  Listener state after pause:");
            System.out.println("    ✓ Running: " + simpleContainer.isRunning());

            result("Listener paused - no new messages will be consumed from Kafka");
            tip("While paused, messages remain on Kafka topic and offsets are NOT committed");
            tip("Consumer group membership is maintained - useful for temporary maintenance");

            // Produce more messages while paused
            step("Producing more messages while listener is paused...");
            produceMessages(TOPIC_SIMPLE, 2);
            result("2 messages produced while listener was paused (will be consumed when resumed)");

            Thread.sleep(1000);

            // DEMONSTRATE 3: Resume the listener
            step("3. Resuming listener...");
            simpleContainer.resume();
            Thread.sleep(1000);

            System.out.println("\n  Listener state after resume:");
            System.out.println("    ✓ Running: " + simpleContainer.isRunning());

            result("Listener resumed - will immediately process the 2 messages that arrived during pause");

            // Wait for the 2 additional messages that arrived during pause
            simpleCountdownLatch = new CountDownLatch(2);

            boolean completed = simpleCountdownLatch.await(5, TimeUnit.SECONDS);
            System.out.println("\n  Total messages consumed: " + simpleMessages.size());

            success("Listener successfully resumed and processed pending messages");

            tip("Resume() returns the listener to normal operation");
            tip("All messages that arrived during pause are processed in order");

            Thread.sleep(1000);

            // DEMONSTRATE 4: Stop the listener (full shutdown)
            step("4. Stopping listener (graceful shutdown)...");
            simpleContainer.stop();
            Thread.sleep(1000);

            System.out.println("\n  Listener state after stop:");
            System.out.println("    ✓ Running: " + simpleContainer.isRunning());

            result("Listener completely stopped");
            tip("When stopped, the consumer group is left (triggers rebalancing)");
            tip("On restart, consumer may need to rejoin group and fetch offsets");

            // DEMONSTRATE 5: Restart the listener
            step("5. Restarting listener after stop...");
            simpleContainer.start();
            Thread.sleep(1000);

            System.out.println("\n  Listener state after restart:");
            System.out.println("    ✓ Running: " + simpleContainer.isRunning());

            result("Listener restarted and back to normal operation");

            success("Listener lifecycle control demonstration completed");
        }

        System.out.println("\n  Practical Use Cases for Lifecycle Control:");
        System.out.println("    • PAUSE: Maintenance window - stop processing without leaving consumer group");
        System.out.println("    • PAUSE: Rate limiting - temporarily pause when downstream system is slow");
        System.out.println("    • STOP: Graceful shutdown - cleanly stop all listeners before app shutdown");
        System.out.println("    • STOP/START: Circuit breaker pattern - stop on errors, restart after recovery");
        System.out.println("    • PAUSE/RESUME: Blue-green deployments - pause old version, restart new version");

        tip("Pause is ideal for temporary maintenance - group membership stays alive");
        tip("Stop is for permanent shutdown or significant state changes");
        tip("Use lifecycle control to implement sophisticated deployment and maintenance strategies");
    }

    private void produceMessages(String topic, int count) throws Exception {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 1; i <= count; i++) {
                String message = "Message #" + i + " | Topic: " + topic + " | Timestamp: " + System.currentTimeMillis();
                System.out.println("[DEBUG] Producing to topic '" + topic + "': " + message);
                producer.send(new ProducerRecord<>(topic, "key-" + i, message));
            }
            producer.flush();
        }

        result("Produced " + count + " messages to '" + topic + "'");
    }

    private void produceMessagesWithHeaders(String topic, int count) throws Exception {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 1; i <= count; i++) {
                String message = "Message #" + i + " with headers";
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key-" + i, message);

                // Add custom headers
                record.headers()
                        .add("correlation-id", ("corr-" + i).getBytes())
                        .add("source", "lesson12".getBytes())
                        .add("priority", (i % 2 == 0 ? "high" : "normal").getBytes());

                producer.send(record);
            }
            producer.flush();
        }

        result("Produced " + count + " messages with custom headers");
    }

    // ========== @KafkaListener Methods ==========
    // These run automatically in Spring's Kafka listener containers

    /**
     * Basic @KafkaListener - Simple message consumption.
     * Demonstrates the simplest usage pattern.
     */
    @KafkaListener(topics = TOPIC_SIMPLE, groupId = GROUP_ID + "-simple", id = "lesson12-simple")
    public void handleSimpleMessage(@Payload String message) {
        System.out.println("[DEBUG] Listener received message: " + message);
        System.out.println("[DEBUG] simpleCountdownLatch is null: " + (simpleCountdownLatch == null));
        System.out.println("[DEBUG] simpleCountdownLatch count: " + (simpleCountdownLatch != null ? simpleCountdownLatch.getCount() : "N/A"));
        simpleMessages.add(message);
        if (simpleCountdownLatch != null) {
            simpleCountdownLatch.countDown();
            System.out.println("[DEBUG] Countdown decremented. Remaining: " + simpleCountdownLatch.getCount());
        }
    }

    /**
     * Advanced @KafkaListener - Multiple topics, manual acknowledgment, metadata access.
     * Shows how to access headers and handle acknowledgment.
     */
    @KafkaListener(
            topics = TOPIC_ADVANCED,
            groupId = GROUP_ID + "-advanced",
            id = "lesson12-advanced"
    )
    public void handleAdvancedMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        String logMessage = String.format(
                "%s | Topic: %s | Partition: %d | Offset: %d",
                message, topic, partition, offset
        );

        advancedMessages.add(logMessage);

        // Process message
        try {
            // Successful processing - acknowledge offset
            if (ack != null) {
                System.out.println("[DEBUG] Manual Acknowledgement - handleAdvancedMessage");
                ack.acknowledge();
            }
        } catch (Exception e) {
            // On error, don't acknowledge - message will be redelivered
            System.err.println("Error processing message: " + e.getMessage());
        }

        if (advancedCountdownLatch != null) {
            advancedCountdownLatch.countDown();
        }
    }

    /**
     * Listener with header extraction - Shows accessing custom headers.
     * Demonstrates practical use of message metadata.
     */
    @KafkaListener(
            topics = TOPIC_SIMPLE,
            groupId = GROUP_ID + "-metadata",
            id = "lesson12-metadata"
    )
    public void handleMessageWithHeaders(
            @Payload String message,
            @Header(name = "correlation-id", required = false) String correlationId,
            @Header(name = "source", required = false) String source,
            @Header(name = "priority", required = false) String priority,
            Acknowledgment ack) {

        // Process message with header context
        String logEntry = String.format(
                "Message: %s | CorrelationId: %s | Source: %s | Priority: %s",
                message, correlationId, source, priority
        );
        System.out.println("[DEBUG] Received message with headers: " + logEntry);

        // Acknowledge after processing
        if (ack != null) {
            System.out.println("[DEBUG] Manual Acknowledgement - handleMessageWithHeaders");
            ack.acknowledge();
        }
    }
}

