# ============================================
# Kafka Producer Learning Agent
# Multi-stage Docker build
# ============================================

# Stage 1: Build
FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /app

# Copy pom.xml first for dependency caching
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source and build
COPY src ./src
RUN mvn clean package -DskipTests -B

# Stage 2: Runtime
FROM eclipse-temurin:21-jre-alpine

LABEL maintainer="Kafka Learning Agent"
LABEL description="Learn Kafka Producer patterns through interactive lessons"
LABEL version="1.0"

WORKDIR /app

# Create non-root user for security
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

# Copy the built JAR
COPY --from=builder /app/target/*.jar app.jar

# Set ownership
RUN chown -R appuser:appgroup /app

USER appuser

# JVM tuning for containers
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -Djava.security.egd=file:/dev/./urandom"

# Expose no ports - this is a CLI app that connects to Kafka
EXPOSE 8090

# Health check (optional, for orchestration)
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD pgrep -f "java" || exit 1

# Run the application
# Note: We disable Spring Boot's Docker Compose integration since infra is external
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar --spring.docker.compose.enabled=false"]
