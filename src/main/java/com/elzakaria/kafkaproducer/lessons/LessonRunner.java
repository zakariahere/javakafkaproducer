package com.elzakaria.kafkaproducer.lessons;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

/**
 * Interactive CLI to select and run Kafka Producer lessons.
 * Automatically discovers all Lesson implementations via Spring DI.
 */
@Component
public class LessonRunner implements CommandLineRunner {

    private final List<Lesson> lessons;

    public LessonRunner(List<Lesson> lessons) {
        this.lessons = lessons.stream()
                .sorted(Comparator.comparingInt(Lesson::getLessonNumber))
                .toList();
    }

    @Override
    public void run(String... args) throws Exception {
        printWelcome();

        Scanner scanner = new Scanner(System.in);

        while (true) {
            printMenu();
            System.out.print("\nEnter lesson number (1-" + lessons.size() + "), 'a' for all, or 'q' to quit: ");

            String input = scanner.nextLine().trim().toLowerCase();

            if (input.equals("q") || input.equals("quit") || input.equals("exit")) {
                System.out.println("\nThank you for learning Kafka Producer! Goodbye.\n");
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
                    System.out.println("\nInvalid lesson number. Please enter 1-" + lessons.size());
                }
            } catch (NumberFormatException e) {
                System.out.println("\nInvalid input. Please enter a number, 'a' for all, or 'q' to quit.");
            }
        }
    }

    private void printWelcome() {
        System.out.println("\n");
        System.out.println("╔══════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                                                                      ║");
        System.out.println("║           KAFKA PRODUCER LEARNING AGENT                              ║");
        System.out.println("║           =============================                              ║");
        System.out.println("║                                                                      ║");
        System.out.println("║   Master Kafka Producer patterns from basics to production-ready    ║");
        System.out.println("║   configurations through hands-on, runnable lessons.                ║");
        System.out.println("║                                                                      ║");
        System.out.println("║   View messages in Kafka UI: http://localhost:8080                  ║");
        System.out.println("║                                                                      ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════╝");
    }

    private void printMenu() {
        System.out.println("\n┌─────────────────────────────────────────────────────────────────────┐");
        System.out.println("│                        AVAILABLE LESSONS                            │");
        System.out.println("├─────────────────────────────────────────────────────────────────────┤");

        for (Lesson lesson : lessons) {
            String entry = String.format("│  %2d. %-62s │",
                    lesson.getLessonNumber(), lesson.getTitle());
            System.out.println(entry);
        }

        System.out.println("├─────────────────────────────────────────────────────────────────────┤");
        System.out.println("│  Module 1 (Lessons 1-3):  Foundations                               │");
        System.out.println("│  Module 2 (Lessons 4-6):  Reliability                               │");
        System.out.println("│  Module 3 (Lessons 7-9):  Advanced Patterns                         │");
        System.out.println("│  Module 4 (Lesson 10):    Production Readiness                      │");
        System.out.println("└─────────────────────────────────────────────────────────────────────┘");
    }

    private void runLesson(int lessonNumber) {
        Lesson lesson = lessons.stream()
                .filter(l -> l.getLessonNumber() == lessonNumber)
                .findFirst()
                .orElse(null);

        if (lesson == null) {
            System.out.println("Lesson " + lessonNumber + " not found.");
            return;
        }

        try {
            lesson.printHeader();
            lesson.run();
            System.out.println("\n" + "=".repeat(70));
            System.out.println("LESSON " + lessonNumber + " COMPLETE");
            System.out.println("=".repeat(70));
        } catch (Exception e) {
            System.out.println("\nLesson failed with error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void runAllLessons() {
        System.out.println("\n>>> Running all lessons sequentially...\n");

        for (Lesson lesson : lessons) {
            try {
                lesson.printHeader();
                lesson.run();
                System.out.println("\n--- Lesson " + lesson.getLessonNumber() + " complete ---\n");
                Thread.sleep(2000); // Brief pause between lessons
            } catch (Exception e) {
                System.out.println("Lesson " + lesson.getLessonNumber() + " failed: " + e.getMessage());
            }
        }

        System.out.println("\n>>> All lessons completed!");
    }
}
