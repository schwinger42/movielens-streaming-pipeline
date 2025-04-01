import json
import logging
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_kafka_connection():
    """Test Kafka connection by sending a simple message"""
    try:
        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Send a test message
        test_message = {"test": "Hello Kafka!", "timestamp": "2025-03-31"}
        producer.send('movies', value=test_message)
        producer.flush()
        
        logger.info("Test message sent successfully!")
        return True
    except Exception as e:
        logger.error(f"Error connecting to Kafka: {e}")
        return False

if __name__ == "__main__":
    test_kafka_connection()
