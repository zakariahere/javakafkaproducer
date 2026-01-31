package com.elzakaria.kafkaproducer.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Sample Order model for demonstrating JSON serialization and business scenarios.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    private String orderId;
    private String customerId;
    private String product;
    private int quantity;
    private BigDecimal price;
    private String status;
    private Instant createdAt;
}
