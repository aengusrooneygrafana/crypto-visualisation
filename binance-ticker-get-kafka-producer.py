import time
import requests
import json
import os
from kafka import KafkaProducer

def data_stream(sleep_interval=1):
 
    # Configure Kafka producer, replace localhost with your Kafka host IP address
    # Original code:    producer = KafkaProducer(bootstrap_servers=['glider.srvs.cloudkafka.com:9094'])
    producer = KafkaProducer(
       bootstrap_servers=['glider.srvs.cloudkafka.com:9094'],
       security_protocol='SASL_SSL',
       sasl_mechanism = 'SCRAM-SHA-512',
       sasl_plain_username = 'xxxx', 
       sasl_plain_password = 'xxxx' 
       #ssl_cafile='ca.pem'
    )  
    
    # Configure Coin Exchange API endpoint and parameters
    url = "https://api.binance.com/api/v3/ticker/price?"
    parameters = 'symbol=BTCUSDT'

    symbol='BTCUSDT' 
    topic='cjxxprym-BTCUSDT-prices'

    while True:
        # Make API request for BTC or ETH prices
        response = requests.get(url, params=parameters)
        data = json.loads(response.text)

        # Process and send BTC data to Kafka
        process_and_send_data(producer, data, symbol, topic) 

        # Sleep for the specified interval before making the next request
        time.sleep(sleep_interval)

def process_and_send_data(producer, data, symbol, topic):
    
    # price_data = data['data'][symbol]['quote']['USD']

    #extracted_data = {
    #    'timestamp': data['status']['timestamp'],
    #    'name': data['data'][symbol]['name'], 
    #    'price': price_data['price'],
    #    'volume_24h': price_data['volume_24h'],
    #    'percent_change_24h': price_data['percent_change_24h']
    #}

    # print(now) 
    print(data) 
    producer.send(topic, json.dumps(data).encode('utf-8'))

if __name__ == "__main__":
    data_stream(sleep_interval=1)
