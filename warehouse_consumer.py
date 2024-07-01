import json
from kafka import KafkaConsumer
import pyodbc
import time

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
    cnxn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}")
except pyodbc.Error as e:
    print(f"Error connecting to database: {e}")
    raise

buffer = []  # buffer to store messages
buffer_size = 100  # buffer size (adjust to your needs)
insert_interval = 120  # insert interval in seconds (adjust to your needs)

last_insert_time = time.time()

def insert_buffer_into_db():
    try:
        cursor = cnxn.cursor()
        for message in buffer:
            message_value = json.loads(message.value.decode('utf-8'))
            event_type = message_value['event_type']
            data = message_value['data']

            if event_type == 'hipment_arrived':
                # Insert into Shipments table
                cursor.execute("INSERT INTO Shipments (CreatedAt) VALUES (GETDATE())")
                shipment_id = cursor.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
                # Insert into ShipmentItems table
                for item in data['items']:
                    cursor.execute("INSERT INTO ShipmentItems (ShipmentID, ItemID, Quantity) VALUES (?,?,?)", shipment_id, item['item_id'], item['quantity'])
            elif event_type == 'item_stored':
                # Insert into ItemStorageLocations table
                cursor.execute("INSERT INTO ItemStorageLocations (ItemID, Location) VALUES (?,?)", data['item_id'], data['location'])
            elif event_type.startswith('shipment_shipped'):
                # Insert into Shipments table
                cursor.execute("INSERT INTO Shipments (CreatedAt) VALUES (GETDATE())")
                shipment_id = cursor.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
                # Insert into ShipmentItems table
                for item in data['items']:
                    cursor.execute("INSERT INTO ShipmentItems (ShipmentID, ItemID, Quantity) VALUES (?,?,?)", shipment_id, item['item_id'], item['quantity'])
        cnxn.commit()
    except pyodbc.Error as e:
        print(f"Error executing SQL query (pyodbc.Error): {e}")
    except Exception as e:
        print(f"Unexpected error processing message: {e}")

def process_message(message):
    global last_insert_time  # 
    buffer.append(message)

    if len(buffer) >= buffer_size or time.time() - last_insert_time >= insert_interval:
        insert_buffer_into_db()
        buffer.clear()
        last_insert_time = time.time()

try:
    for message in consumer:
        process_message(message)
finally:
    # Close the database connection
    cnxn.close()