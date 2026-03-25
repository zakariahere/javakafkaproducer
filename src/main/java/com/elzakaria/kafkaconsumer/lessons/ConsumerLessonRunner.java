package com.elzakaria.kafkaconsumer.lessons;

import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

/**
 * Interactive CLI to select and run Kafka Consumer lessons.
 * Automatically discovers all Lesson implementations via Spring DI.
 * Note: Controlled by ApplicationLauncher, not auto-run.
 */
@Service("consumerLessonRunner")
public class ConsumerLessonRunner {

    private final List<Lesson> lessons;

    public ConsumerLessonRunner(List<Lesson> lessons) {
        this.lessons = lessons.stream()
                .sorted(Comparator.comparingInt(Lesson::getLessonNumber))
                .toList();
    }

    public void run(String... args) throws Exception {
        printWelcome();

        Scanner scanner = new Scanner(System.in);

        while (true) {
            printMenu();
            System.out.print("\nEnter lesson number (1-" + lessons.size() + "), 'a' for all, or 'q' to quit: ");

            String input = scanner.nextLine().trim().toLowerCase();

            if (input.equals("q") || input.equals("quit") || input.equals("exit")) {
                System.out.println("\nThank you for learning Kafka Consumer! Goodbye.\n");
                break;
            }

            if (input.equals("a") || input.equals("all")) {
                runAllLessons();
                continue;
            }

            try {
                int lessonNum = Integer.parseInt(input);
                if (lessonNum >= 1 && lessonNum <= lessons.size()) {
                    runLesson(lessonNum);
                } else {
                    System.out.println("Invalid lesson number. Please enter 1-" + lessons.size());
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a number, 'a', or 'q'");
            }
        }

        // Note: Do NOT close scanner - System.in is shared with ApplicationLauncher
    }

    private void printWelcome() {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("Welcome to Kafka Consumer Lessons!");
        System.out.println("=".repeat(70));
        System.out.println("""
            This interactive course teaches you how to properly configure,
            use, and operate Kafka Consumers with best practices.

            Topics covered:
            - Consumer basics and polling model
            - Consumer groups and rebalancing
            - Offset management and commit strategies
            - Deserialization and type safety
            - Error handling and resilience
            - State management and exactly-once semantics
            - Concurrency and threading models
            - Performance tuning and metrics
            - Advanced patterns and integration
            - Production readiness
            - Transactions and exactly-once semantics
            - Real-world scenarios

            Prerequisites: Running Kafka broker at localhost:9092
            """);
    }

    private void printMenu() {
        System.out.println("\n" + "-".repeat(70));
        System.out.println("Available Lessons:");
        System.out.println("-".repeat(70));

        for (Lesson lesson : lessons) {
            System.out.printf(" [%2d] %-50s%n", lesson.getLessonNumber(), lesson.getTitle());
        }
    }

    private void runLesson(int lessonNum) {
        Lesson lesson = lessons.stream()
                .filter(l -> l.getLessonNumber() == lessonNum)
                .findFirst()
                .orElse(null);

        if (lesson != null) {
            try {
                lesson.printHeader();
                lesson.run();
                System.out.println("\n" + "-".repeat(70));
                System.out.println("Lesson " + lessonNum + " completed!");
                System.out.println("-".repeat(70));
            } catch (Exception e) {
                System.out.println("\n[ERROR] Lesson execution failed:");
                e.printStackTrace();
            }
        }
    }

    private void runAllLessons() {
        System.out.println("\nRunning all " + lessons.size() + " lessons...\n");

        for (Lesson lesson : lessons) {
            System.out.println("\n" + "█".repeat(70));
            try {
                lesson.printHeader();
                lesson.run();
                System.out.println("\n✓ Lesson " + lesson.getLessonNumber() + " completed");
            } catch (Exception e) {
                System.out.println("\n✗ Lesson " + lesson.getLessonNumber() + " failed:");
                e.printStackTrace();
            }
        }

        System.out.println("\n" + "█".repeat(70));
        System.out.println("All lessons completed!");
    }
}

