package com.jugtours.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.time.Duration;
import java.util.Properties;

public class WikimediaProducer {
    private static final String TOPIC_NAME = "wikimedia";
    private static final String WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = createKafkaProducer();

        // Add shutdown hook to close the producer gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(WIKIMEDIA_STREAM_URL))
                .timeout(Duration.ofSeconds(10))
                .build();

        // Start streaming from Wikimedia and produce to Kafka
        client.sendAsync(request, HttpResponse.BodyHandlers.ofLines())
                .thenAccept(response -> response.body().forEach(line -> {
                    if (line.startsWith("data: ")) {
                        String event = line.substring(6).trim();
                        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, event);

                        producer.send(record, (metadata, exception) -> {
                            if (exception == null) {
                                System.out.printf("Sent to Kafka: Topic=%s, Offset=%d%n",
                                        metadata.topic(), metadata.offset());
                            } else {
                                System.err.printf("Error sending message: %s%n", exception.getMessage());
                                exception.printStackTrace();
                            }
                        });
                    }
                }))
                .exceptionally(ex -> {
                    System.err.printf("Error during HTTP request: %s%n", ex.getMessage());
                    ex.printStackTrace();
                    return null;
                })
                .join();
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        return new KafkaProducer<>(props);
    }
}
