# Stage 1: Build the application
FROM gradle:7.6.1-jdk17 AS builder

WORKDIR /app

# Copy project files
COPY . .

# Build the fat JAR
RUN gradle clean fatJar --no-daemon

# Stage 2: Create the runtime image
FROM openjdk:17-slim

WORKDIR /app

# Copy the fat JAR (update name if necessary)
COPY --from=builder /app/build/libs/*-all.jar app.jar

# Environment variables
ENV APP_MODE=producer
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV OPENSEARCH_HOST=https://opensearch:9200

# Define the ENTRYPOINT with the Main class
ENTRYPOINT ["java", "-cp", "app.jar", "com.jugtours.Main"]
