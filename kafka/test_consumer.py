import json
import logging
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_kafka_consumer():
    """Test Kafka consumer by listening for messages"""
    try:
        # Create consumer
        consumer = KafkaConsumer(
            'movies',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=10000  # Timeout after 10 seconds of no messages
        )
        
        logger.info("Consumer started, waiting for messages...")
        
        # Listen for messages
        for message in consumer:
            logger.info(f"Received message: {message.value}")
        
        logger.info("No more messages to consume")
        return True
    except Exception as e:
        logger.error(f"Error consuming from Kafka: {e}")
        return False

if __name__ == "__main__":
    test_kafka_consumer()
