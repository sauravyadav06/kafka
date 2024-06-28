import json
from kafka import KafkaConsumer

# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'warehouse_events'
group_id = 'warehouse_consumer_group'

# Create a Kafka consumer instance
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    group_id=group_id,
    value_deserializer=lambda m:json.loads(m),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)   

# Define a function to process a message
def process_message(message):
    print(message.value)
    
    values =message.value #json.load(message.value)
    event_type =values['event_type']
    data = values['data']
    
    if event_type == 'shipment_arrived':
        print(f"Shipment arrived: {data['shipment_id']}")
        # Process shipment arrival event
        process_shipment_arrival(data)
    elif event_type == 'item_stored':
        print(f"Item stored: {data['item_id']}")
        # Process item storage event
        process_item_storage(data)
    elif event_type.startswith('shipment_shipped'):
        print(f"Shipment shipped: {data['shipment_id']}")
        # Process shipment shipping event
        process_shipment_shipping(data)
    else:
        print(f"Unknown event type: {event_type}")

# Define functions to process specific events
def process_shipment_arrival(data):
    # Process shipment arrival event logic here
    pass

def process_item_storage(data):
    # Process item storage event logic here
    pass

def process_shipment_shipping(data):
    # Process shipment shipping event logic here
    pass

# Consume messages from the topic
for message in consumer:
    # print(message)
    process_message(message)