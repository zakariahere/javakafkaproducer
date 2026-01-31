# ============================================
# Kafka Producer Learning Agent
# Multi-stage Docker build
# ============================================

# Java version - update if needed based on available images
ARG JAVA_VERSION=25

# Stage 1: Build with Maven and JDK
FROM eclipse-temurin:${JAVA_VERSION}-jdk AS builder

WORKDIR /app

# Install Maven
RUN apt-get update && \
    apt-get install -y maven && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy pom.xml first for dependency caching
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source and build
COPY src ./src
RUN mvn clean package -DskipTests -B

# Stage 2: Runtime with JRE
FROM eclipse-temurin:${JAVA_VERSION}-jre

LABEL maintainer="Kafka Learning Agent"
LABEL description="Learn Kafka Producer patterns through interactive lessons"
LABEL version="1.0"

WORKDIR /app

# Create non-root user for security
RUN groupadd -g 1001 appgroup && \
    useradd -u 1001 -g appgroup -s /bin/bash appuser

# Copy the built JAR
COPY --from=builder /app/target/*.jar app.jar

# Set ownership
RUN chown -R appuser:appgroup /app

USER appuser

# JVM tuning for containers
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -Djava.security.egd=file:/dev/./urandom"

# Health check (optional, for orchestration)
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD pgrep -f "java" || exit 1

# Run the application
# Note: We disable Spring Boot's Docker Compose integration since infra is external
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar --spring.docker.compose.enabled=false"]
