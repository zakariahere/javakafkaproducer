package com.elzakaria.kafkaproducer.lessons.lesson03_partitioning;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Custom partitioner that routes messages based on region prefix in the key.
 *
 * Routing logic:
 * - Keys starting with "US-"   -> Partition 0
 * - Keys starting with "EU-"   -> Partition 1
 * - Keys starting with "APAC-" -> Partition 2
 * - All others                 -> Partition 3
 */
public class RegionPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value,
                         byte[] valueBytes, Cluster cluster) {

        int numPartitions = cluster.partitionCountForTopic(topic);

        if (key == null) {
            // Fallback to last partition for null keys
            return numPartitions - 1;
        }

        String keyStr = key.toString();

        if (keyStr.startsWith("US-")) {
            return 0 % numPartitions;
        } else if (keyStr.startsWith("EU-")) {
            return 1 % numPartitions;
        } else if (keyStr.startsWith("APAC-")) {
            return 2 % numPartitions;
        } else {
            return 3 % numPartitions;
        }
    }

    @Override
    public void close() {
        // No resources to clean up
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed
    }
}
