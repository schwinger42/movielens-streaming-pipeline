import json
import logging
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_topic(topic_name, max_messages=10):
    """Verify data flow by consuming a limited number of messages"""
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',  # Get only new messages
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=20000  # Timeout after 20 seconds of no messages
    )
    
    logger.info(f"Started consumer for {topic_name}, waiting for messages...")
    
    count = 0
    for message in consumer:
        logger.info(f"Received from {topic_name}: {message.value}")
        count += 1
        if count >= max_messages:
            break
    
    logger.info(f"Received {count} messages from {topic_name}")
    consumer.close()

if __name__ == "__main__":
    # Verify both topics
    verify_topic('movies', 5)
    verify_topic('ratings', 5)
