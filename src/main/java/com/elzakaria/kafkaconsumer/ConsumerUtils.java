package com.elzakaria.kafkaconsumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Map;

public class ConsumerUtils {
  public static void createTopics(int numPartitions, String... topics) {
    System.out.println("Step -1: Ensuring topics are clean by deleting and recreating with " + numPartitions + " partitions");

    try (AdminClient adminClient = AdminClient.create(Map.of(
        "bootstrap.servers", "localhost:9092"
    ))) {
      for (String topicName : topics) {
        // Delete topic if it exists
        try {
          adminClient.deleteTopics(Collections.singleton(topicName)).all().get(5, java.util.concurrent.TimeUnit.SECONDS);
          System.out.println("Deleted topic '" + topicName + "' (if existed)");
          Thread.sleep(1000);
        } catch (Exception e) {
          // Ignore if topic does not exist
        }
        // Recreate topic
        try {
          NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
          adminClient.createTopics(Collections.singleton(newTopic)).all().get(5, java.util.concurrent.TimeUnit.SECONDS);
          System.out.println("Topic '" + topicName + "' created with " + numPartitions + " partitions");
        } catch (Exception e) {
          System.out.println("Could not create topic '" + topicName + "': " + e.getMessage());
        }
      }
    } catch (Exception e) {
      System.out.println("Could not create topics: " + e.getMessage());
    }
  }
}
