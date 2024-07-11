import json
from kafka import KafkaProducer
import time

# Kafka producer configuration
bootstrap_servers ='localhost:9092'
topic_name = 'warehouse_events'

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Define a function to send a message to the Kafka topic
def send_message(event_type, data):
    message = {
        'event_type': event_type,
        'data': data
    }
    producer.send(topic_name, value=json.dumps(message).encode('utf-8'))

# Define the shipment data
shipment_data = {
    'shipment_id': 123,
    'items': [
        {'item_id': 1, 'quantity': 10}
        
    ]
}

while True:
    # Send 20 messages in a batch
    for i in range(20):
        send_message('shipment_shipped {}'.format(i), shipment_data)
    
    # Wait for 30 seconds before sending the next batch
    time.sleep(60)

# Close the producer (not reached in this example, but good practice)
producer.close()