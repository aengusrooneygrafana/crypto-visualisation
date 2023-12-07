import time
import requests
import json
import os
from kafka import KafkaConsumer 

def data_stream(sleep_interval=1):
 
    # Configure Kafka consumer, replace localhost with your Kafka host IP address
    # Original code:    producer = KafkaProducer(bootstrap_servers=['glider.srvs.cloudkafka.com:9094'])
    
    consumer = KafkaConsumer(
       bootstrap_servers=['glider.srvs.cloudkafka.com:9094'],
       security_protocol="SASL_SSL",
       sasl_mechanism = 'SCRAM-SHA-512',
       sasl_plain_username = 'xxxx', 
       sasl_plain_password = 'xxxx', 
       value_deserializer = lambda v: json.loads(v.decode('utf-8')),
       auto_offset_reset='earliest'
    )

    while True: 
        consumer.subscribe(topics='cjxxprym-BTCUSDT-prices') 
        for message in consumer:
          print ("%d:%d: v=%s" % (message.partition,
                                  message.offset,
                                  message.value))

        time.sleep(sleep_interval)


if __name__ == "__main__":
    data_stream(sleep_interval=1)
