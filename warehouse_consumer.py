import json
from kafka import KafkaConsumer
import pyodbc

# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'warehouse_events'
group_id = 'warehouse_events_consumer'

# Create a Kafka consumer instance
try:
    consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers, group_id=group_id)
    consumer.subscribe([topic_name])
except Exception as e:
    print(f"Error creating Kafka consumer: {e}")
    raise

# Microsoft SQL Server connection settings
server = 'PSL\\SQLEXPRESS'
database = 'kafka'
username = 'sa'
password = 'test!23'

# Create a connection to the database
try:
    cnxn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={'PSL\\SQLEXPRESS'};DATABASE={'kafka'};UID={'sa'};PWD={'test!23'}")
except pyodbc.Error as e:
    print(f"Error connecting to database: {e}")
    raise


try:
    # Define a function to process messages from the Kafka topic
    def process_message(message):
        try:
            message_value = json.loads(message.value.decode('utf-8'))
            event_type = message_value['event_type']
            data = message_value['data']

            # Create a cursor object
            cursor = cnxn.cursor()

            # Insert data into the appropriate table based on the event type
            if event_type == 'shipment_arrived':
                # Insert into Shipments table
                cursor.execute("INSERT INTO Shipments (ShipmentID, CreatedAt) VALUES (?, GETDATE())", data['shipment_id'])
                # Insert into ShipmentItems table
                for item in data['items']:
                    cursor.execute("INSERT INTO ShipmentItems (ShipmentID, ItemID, Quantity) VALUES (?,?,?)", data['shipment_id'], item['item_id'], item['quantity'])
            elif event_type == 'item_stored':
                # Insert into ItemStorageLocations table
                cursor.execute("INSERT INTO ItemStorageLocations (ItemID, Location) VALUES (?,?)", data['item_id'], data['location'])
            elif event_type.startswith('shipment_shipped'):
                # Insert into Shipments table
                cursor.execute("INSERT INTO Shipments (ShipmentID, CreatedAt) VALUES (?, GETDATE())", data['shipment_id'])
                # Insert into ShipmentItems table
                for item in data['items']:
                    cursor.execute("INSERT INTO ShipmentItems (ShipmentID, ItemID, Quantity) VALUES (?,?,?)", data['shipment_id'], item['item_id'], item['quantity'])

            # Commit the transaction
            cnxn.commit()
        except KeyError as e:
            print(f"Error processing message: {e}")
        except pyodbc.Error as e:
            print(f"Error executing SQL query: {e}")

    # Consume messages from the Kafka topic
    for message in consumer:
        process_message(message)
finally:
    # Close the database connection
    cnxn.close()