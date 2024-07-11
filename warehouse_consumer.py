import json
from kafka import KafkaConsumer
import pyodbc
import time
import logging
import threading
from queue import Queue
from contextlib import contextmanager

# Configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'warehouse_events'
group_id = 'warehouse_events_consumer'
buffer_size = 30
insert_interval = 40

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnection:
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.cnxn = None

    @contextmanager
    def connect(self):
        try:
            self.cnxn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}")
            yield self.cnxn
        except pyodbc.Error as e:
            logger.error(f"Error connecting to database: {e}")
            raise
        finally:
            if self.cnxn:
                self.cnxn.close()

class MessageProcessor:
    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.buffer = Queue(maxsize=buffer_size)

    def process_message(self, message):
        self.buffer.put(message)
        if self.buffer.qsize() >= buffer_size:
            self.insert_buffer_into_db()

    def insert_buffer_into_db(self):
        with self.db_connection.connect() as cnxn:
            cursor = cnxn.cursor()
            while not self.buffer.empty():
                message = self.buffer.get()
                message_value = json.loads(message.value.decode('utf-8'))
                event_type = message_value['event_type']
                data = message_value['data']

                if event_type == 'shipment_arrived':
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

class TimerThread(threading.Thread):
    def __init__(self, message_processor, insert_interval):
        super().__init__()
        self.message_processor = message_processor
        self.insert_interval = insert_interval

    def run(self):
        while True:
            time.sleep(self.insert_interval)
            self.message_processor.insert_buffer_into_db()

class KafkaConsumerThread(threading.Thread):
    def __init__(self, bootstrap_servers, topic_name, group_id, message_processor):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.group_id = group_id
        self.message_processor = message_processor
        self.consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers, group_id=group_id)
        self.consumer.subscribe([topic_name])

    def run(self):
        try:
            for message in self.consumer:
                self.message_processor.process_message(message)
        except Exception as e:
            logger.error(f"Error consuming message: {e}")

if __name__ == '__main__':
    db_connection = DatabaseConnection('PSL\\SQLEXPRESS', 'kafka', 'sa', 'test!23')
    message_processor = MessageProcessor(db_connection)
    timer_thread = TimerThread(message_processor, insert_interval)
    timer_thread.daemon = True
    timer_thread.start()

    kafka_consumer_thread = KafkaConsumerThread(bootstrap_servers, topic_name, group_id, message_processor)
    kafka_consumer_thread.start()

    kafka_consumer_thread.join()



    