import json
from kafka import KafkaProducer

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

# Example usage:
# Send a message when a new shipment arrives
shipment_data = {
    'shipment_id': 123,
    'items': [
        {'item_id': 1, 'quantity': 10},
        {'item_id': 2, 'quantity': 20}
    ]
}
send_message('shipment_arrived', shipment_data)

# Send a message when an item is stored in the warehouse
item_data = {
    'item_id': 1,
    'location': 'Aisle 1, Shelf 2'
}
send_message('item_stored', item_data)

# Send a message when an item is shipped out
shipment_data = {
    'shipment_id': 123,
    'items': [
        {'item_id': 1, 'quantity': 5}
    ]
}
import time
counter = 0
while(1==1):
    counter +=1
    print(counter)
    send_message('shipment_shipped {}'.format(counter), shipment_data)
    time.sleep(1)

# Close the producer
producer.close()



