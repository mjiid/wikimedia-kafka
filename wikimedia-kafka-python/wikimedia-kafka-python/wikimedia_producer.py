from confluent_kafka import Producer
import json
from sseclient import SSEClient
from typing import Dict, Any

class WikimediaKafkaProducer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092', topic: str = 'wikimedia'):
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all',
            'retries': 3
        })
        self.stream_url = "https://stream.wikimedia.org/v2/stream/recentchange"

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def process_event(self, event_data: Dict[Any, Any]) -> None:
        try:
            self.producer.produce(
                self.topic,
                value=json.dumps(event_data).encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.poll(0)  # Trigger delivery reports
            print(f"Sent to Kafka: {event_data['user']} - {event_data['title']}")
        except Exception as e:
            print(f"Error sending to Kafka: {str(e)}")

    def start_streaming(self) -> None:
        print("Starting Kafka Producer...")
        try:
            for event in SSEClient(self.stream_url):
                if event.event == "message":
                    try:
                        data = json.loads(event.data)
                        self.process_event(data)
                    except json.JSONDecodeError as e:
                        print(f"JSON parsing error: {str(e)}")
                    except Exception as e:
                        print(f"Unexpected error: {str(e)}")
        except KeyboardInterrupt:
            print("\nStreaming interrupted by user")
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        """Clean up resources."""
        self.producer.flush() 
        print("Kafka producer closed")


def main():
    producer = WikimediaKafkaProducer()
    producer.start_streaming()


if __name__ == '__main__':
    main()