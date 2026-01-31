package com.elzakaria.kafkaproducer.lessons;

/**
 * Base interface for all Kafka Producer lessons.
 * Each lesson demonstrates a specific concept or pattern.
 */
public interface Lesson {

    /**
     * Execute the lesson demonstration.
     */
    void run() throws Exception;

    /**
     * Get the lesson number (1-10).
     */
    int getLessonNumber();

    /**
     * Get a short title for the lesson.
     */
    String getTitle();

    /**
     * Get a detailed description of what this lesson teaches.
     */
    String getDescription();

    /**
     * Print a formatted header for the lesson.
     */
    default void printHeader() {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("LESSON " + getLessonNumber() + ": " + getTitle());
        System.out.println("=".repeat(70));
        System.out.println(getDescription());
        System.out.println("-".repeat(70) + "\n");
    }

    /**
     * Print a concept explanation.
     */
    default void explain(String concept) {
        System.out.println("\n>> " + concept);
    }

    /**
     * Print a step in the demonstration.
     */
    default void step(String description) {
        System.out.println("\n[STEP] " + description);
    }

    /**
     * Print the result of an operation.
     */
    default void result(String message) {
        System.out.println("  -> " + message);
    }

    /**
     * Print a success message.
     */
    default void success(String message) {
        System.out.println("  [OK] " + message);
    }

    /**
     * Print an error message.
     */
    default void error(String message) {
        System.out.println("  [ERROR] " + message);
    }

    /**
     * Print a tip or best practice.
     */
    default void tip(String message) {
        System.out.println("\n[TIP] " + message);
    }

    /**
     * Wait for a specified duration with a message.
     */
    default void waitFor(int seconds, String reason) throws InterruptedException {
        System.out.println("\n  Waiting " + seconds + "s: " + reason);
        Thread.sleep(seconds * 1000L);
    }
}
