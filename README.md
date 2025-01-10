# Streaming Data from Wikimedia to OpenSearch Dashboard using Kafka

## Overview
This project demonstrates how to stream real-time data from Wikimedia's event stream to an OpenSearch dashboard using Apache Kafka as the intermediary.

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