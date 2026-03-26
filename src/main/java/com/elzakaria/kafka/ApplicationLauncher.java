package com.elzakaria.kafka;

import com.elzakaria.kafkaconsumer.lessons.ConsumerLessonRunner;
import com.elzakaria.kafkaproducer.lessons.ProducerLessonRunner;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Scanner;

/**
 * Main application launcher that provides menu to choose between
 * Kafka Producer Lessons and Kafka Consumer Lessons.
 */
@Component
public class ApplicationLauncher implements CommandLineRunner {

    private final ProducerLessonRunner producerLessonRunner;
    private final ConsumerLessonRunner consumerLessonRunner;

    public ApplicationLauncher(
            @Qualifier("producerLessonRunner") ProducerLessonRunner producerLessonRunner,
            @Qualifier("consumerLessonRunner") ConsumerLessonRunner consumerLessonRunner) {
        this.producerLessonRunner = producerLessonRunner;
        this.consumerLessonRunner = consumerLessonRunner;
    }

    @Override
    public void run(String... args) throws Exception {
        Scanner scanner = new Scanner(System.in);

        printMainMenu();

      label:
      while (true) {
          System.out.print("\nEnter your choice (1=Producer, 2=Consumer, 'q'=Quit): ");
          String input = scanner.nextLine().trim().toLowerCase();

        switch (input) {
          case "q":
          case "quit":
            System.out.println("\n👋 Thank you for learning Kafka! Goodbye.\n");
            break label;
          case "1":
            producerLessonRunner.run(args);
            printMainMenu();
            break;
          case "2":
            consumerLessonRunner.run(args);
            printMainMenu();
            break;
          default:
            System.out.println("❌ Invalid choice. Please enter 1, 2, or 'q'.");
            break;
        }

      }
    }

    private void printMainMenu() {
        System.out.println("\n");
        System.out.println("╔════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                                                                            ║");
        System.out.println("║               🎓 KAFKA LEARNING AGENT - LESSON SELECTOR 🎓                 ║");
        System.out.println("║                                                                            ║");
        System.out.println("╚════════════════════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("┌────────────────────────────────────────────────────────────────────────────┐");
        System.out.println("│  Which lessons would you like to explore?                                 │");
        System.out.println("├────────────────────────────────────────────────────────────────────────────┤");
        System.out.println("│                                                                            │");
        System.out.println("│  [1] 📤 KAFKA PRODUCER LESSONS (11 Lessons)                               │");
        System.out.println("│      Learn to send messages with confidence                               │");
        System.out.println("│      Topics: Basics, Partitioning, Serialization, Callbacks,             │");
        System.out.println("│              Error Handling, Transactions, Batching, etc.                │");
        System.out.println("│                                                                            │");
        System.out.println("│  [2] 📥 KAFKA CONSUMER LESSONS (12 Lessons)                               │");
        System.out.println("│      Master consuming messages reliably                                   │");
        System.out.println("│      Topics: Polling, Groups, Offsets, Deserialization, Error Handling,  │");
        System.out.println("│              State Management, Concurrency, Performance, Production, etc. │");
        System.out.println("│                                                                            │");
        System.out.println("│  [q] Quit the application                                                 │");
        System.out.println("│                                                                            │");
        System.out.println("└────────────────────────────────────────────────────────────────────────────┘");
    }
}


