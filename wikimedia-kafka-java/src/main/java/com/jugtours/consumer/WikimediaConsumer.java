package com.jugtours.consumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class WikimediaConsumer {
    private static final String TOPIC_NAME = "wikimedia_recentchange";
    private static final String OPENSEARCH_HOST = "https://localhost:9200"; 
    private static final String INDEX_NAME = "wikimedia";
    private static final String OPENSEARCH_USERNAME = "admin";
    private static final String OPENSEARCH_PASSWORD = "ksdqjfkdsllAdjqfiezksdfjjhrsdf:2sda";

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException, KeyManagementException, KeyStoreException {
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        RestClient restClient = createOpenSearchClient();

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                consumer.close();
                restClient.close();
                System.out.println("Closed consumer and OpenSearch client.");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        // Poll and index records
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String jsonString = record.value();

                // Index the record into OpenSearch
                Request request = new Request("POST", "/" + INDEX_NAME + "/_doc");
                request.setJsonEntity(jsonString);

                try {
                    Response response = restClient.performRequest(request);
                    System.out.println("Indexed record: " + response.getStatusLine().getStatusCode());
                } catch (Exception e) {
                    System.err.println("Error indexing record: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); 
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "wikimedia-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); 
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); 

    
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        return consumer;
    }

    private static RestClient createOpenSearchClient() throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException {
        // Setup basic authentication credentials
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
        );
    
        // Disable SSL verification
        SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();
        sslContextBuilder.loadTrustMaterial(null, (certificate, authType) -> true); // Trust all certificates
    
        RestClientBuilder builder = RestClient.builder(HttpHost.create(OPENSEARCH_HOST))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                    try {
                        return httpAsyncClientBuilder
                                .setDefaultCredentialsProvider(credentialsProvider)
                                .setSSLContext(sslContextBuilder.build())
                                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                    } catch (NoSuchAlgorithmException | KeyManagementException e) {
                        throw new RuntimeException("Failed to set SSL context", e);
                    }
                });
    
        return builder.build();
    }
    
}
