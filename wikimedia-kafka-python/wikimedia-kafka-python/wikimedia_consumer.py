from typing import Dict, Any
import json
from confluent_kafka import Consumer, KafkaError
from opensearchpy import OpenSearch, OpenSearchException

class WikimediaOpenSearchConsumer:
    def __init__(
        self,
        kafka_bootstrap_servers: str = 'localhost:9092',
        kafka_topic: str = 'wikimedia',
        opensearch_host: str = 'localhost',
        opensearch_port: int = 9200,
        opensearch_username: str = 'admin',
        opensearch_password: str = 'ksdqjfkdsllAdjqfiezksdfjjhrsdf:2sda'
    ):
        
        self.kafka_topic = kafka_topic
        self.index_name = "wikimedia"
        
        # Initialize Kafka consumer
        self.consumer = self._setup_consumer(kafka_bootstrap_servers)
        
        # Initialize OpenSearch client
        self.os_client = self._setup_opensearch(
            opensearch_host,
            opensearch_port,
            opensearch_username,
            opensearch_password
        )
        
        # Ensure index exists
        self._ensure_index_exists()

    def _setup_consumer(self, bootstrap_servers: str) -> Consumer:
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'wikimedia-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        consumer = Consumer(conf)
        consumer.subscribe([self.kafka_topic])
        return consumer

    def _setup_opensearch(
        self,
        host: str,
        port: int,
        username: str,
        password: str
    ) -> OpenSearch:
        return OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=(username, password),
            use_ssl=True,
            verify_certs=False,
            ssl_show_warn=False
        )

    def _ensure_index_exists(self) -> None:
        try:
            if not self.os_client.indices.exists(index=self.index_name):
                self.os_client.indices.create(index=self.index_name)
                print(f"Created new index: {self.index_name}")
        except OpenSearchException as e:
            print(f"Error creating index: {str(e)}")
            raise

    def process_message(self, data: Dict[str, Any]) -> None:
        try:
            self.os_client.index(index=self.index_name, body=data)
            print(
                f"Indexed message - User: {data['user']}, Title: {data['title']}"
            )
        except OpenSearchException as e:
            print(f"Error indexing message: {str(e)}")
        except KeyError as e:
            print(f"Missing required field in message: {str(e)}")

    def start_consuming(self) -> None:
        print("Starting Kafka Consumer...")
        try:
            while True:
                msg = self.consumer.poll(1.0) 
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print("Reached end of partition")
                    else:
                        print(f"Kafka error: {msg.error()}")
                    continue

                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    self.process_message(value)
                except json.JSONDecodeError as e:
                    print(f"JSON parsing error: {str(e)}")
                except Exception as e:
                    print(f"Unexpected error processing message: {str(e)}")

        except KeyboardInterrupt:
            print("Consumer interrupted by user")
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        self.consumer.close()
        print("Kafka consumer closed")


def main():
    consumer = WikimediaOpenSearchConsumer()
    consumer.start_consuming()

if __name__ == '__main__':
    main()