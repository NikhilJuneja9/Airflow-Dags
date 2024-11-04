
from kafka import KafkaProducer
import json 
from json import dumps
import pandas as pd
import time
KAFKA_TOPIC = "MOVE-DATA"
# KAFKA_SERVER = ["35.239.61.131:6667"]
kafka = "hadoop.ddns.net:6667"
PATH = '/home/4px/radar_data/radar_data_new.csv'

import sys 
# producer = KafkaProducer(
#     bootstrap_servers = KAFKA_SERVER,
#     security_protocol="PLAINTEXT",
#     value_serializer=lambda x: json.dumps(x).encode("utf-8"),
#     key_serializer = lambda x: json.dumps(x).encode("utf-8"),
#     api_version=(0, 10, 1),
#     request_timeout_ms=5000)

    
def get_kafka_producer_client():
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka,
            api_version=(0, 10, 1),
            security_protocol="PLAINTEXT",
            value_serializer=lambda x: dumps(x).encode("utf-8"),
            request_timeout_ms=30000
        )
        print("connection-done")
        return producer
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def on_send_success(record_metadata):
    print(f"Message delivered to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Failed to deliver message: {excp}")
    

######################################################################################################################################
# MAIN
######################################################################################################################################
radar_data = pd.read_csv(PATH,delimiter=",")
leng = len(radar_data)
for i in range(0,leng):
    message = radar_data.iloc[i][:].to_json()
    
    producer = get_kafka_producer_client()
    producer.send(KAFKA_TOPIC,value = message).add_callback(on_send_success).add_errback(on_send_error)
    time.sleep(30)
    

# for i in range(10):
#     data = {"Name":i}
#     producer = get_kafka_producer_client()
#     producer.send("vertica-topic",value = data,partition=1).add_callback(on_send_success).add_errback(on_send_error)
#     time.sleep(30)

# producer.flush()
