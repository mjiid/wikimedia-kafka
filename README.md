# Streaming Data from Wikimedia to OpenSearch Dashboard using Kafka

## Overview
This project demonstrates how to stream real-time data from Wikimedia's event stream to an OpenSearch dashboard using Apache Kafka as the intermediary. The implementation is provided in two programming languages: Java and Python.

---

## Prerequisites

1. **Docker and Docker Compose**: Ensure Docker is installed for containerized services.
2. **Kafka**: Use a Kafka broker setup, either locally or through Docker.
3. **OpenSearch**: An OpenSearch instance to visualize the data.
4. **Wikimedia Stream**: Access to Wikimedia's [Streams](https://stream.wikimedia.org/v2/stream/recentchange).

---

## Architecture

1. **Wikimedia Event Source**: The API provides a continuous stream of events.
2. **Kafka Producer**: A producer reads the event stream and publishes messages to a Kafka topic.
3. **Kafka Consumer**: A consumer fetches data from the Kafka topic and indexes it into OpenSearch.
4. **OpenSearch Dashboard**: Visualizes the ingested data.

---

## Steps to Run the Pipeline

### Java Implementation

#### 1. Run the Java Application
1. Navigate to the `wikimedia-kafka-java` directory:
   ```bash
   cd wikimedia-kafka-java
   ```
2. Start all services using Docker Compose:
   ```bash
   docker compose up -d
   ```

### Python Implementation

#### 1. Run the Python Application
1. Navigate to the `wikimedia-kafka-python` directory:
   ```bash
   cd wikimedia-kafka-python
   ```
2. Start all services using Docker Compose:
   ```bash
   docker compose up -d
   ```
3. Start the producer And the Consumer independently:
   ```bash
   python wikimedia_producer.py
   python wikimedia_consumer.py
   ```

---

## OpenSearch Dashboard

1. Access OpenSearch Dashboard:
   ```
   http://localhost:5601
   ```
2. Create an index pattern corresponding to the Kafka topic.
3. Visualize the data.

---