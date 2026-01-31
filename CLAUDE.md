# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spring Boot 3.5.10 Kafka Producer application using Java 25 and Maven. Currently a skeleton project with Kafka dependencies but no producer/consumer implementation yet.

## Build Commands

```bash
# Compile
./mvnw clean compile

# Run tests
./mvnw test

# Run a single test class
./mvnw test -Dtest=KafkaproducerApplicationTests

# Run a single test method
./mvnw test -Dtest=KafkaproducerApplicationTests#contextLoads

# Package JAR
./mvnw clean package

# Run application
./mvnw spring-boot:run

# Build Docker image
./mvnw spring-boot:build-image
```

On Windows, use `mvnw.cmd` instead of `./mvnw`.

## Architecture

- **Entry Point**: `src/main/java/com/elzakaria/kafkaproducer/KafkaproducerApplication.java`
- **Package Structure**: `com.elzakaria.kafkaproducer`
- **Configuration**: `src/main/resources/application.properties`
- **Docker Compose**: `compose.yaml` (currently empty, needs Kafka broker configuration)

## Key Dependencies

- Spring Boot Starter with Spring Kafka for messaging
- Lombok for boilerplate reduction (use annotations like `@Data`, `@Builder`)
- Spring Boot DevTools for hot reload during development
- Spring Boot Docker Compose support for local services

## Development Notes

- The `compose.yaml` is empty - a Kafka broker must be added before running the application
- Lombok annotation processor is configured in the Maven build
- Tests use Spring Kafka Test utilities for Kafka integration testing
