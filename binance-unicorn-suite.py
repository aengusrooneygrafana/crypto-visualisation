from aiokafka import AIOKafkaProducer
from aiokafka.helpers import create_ssl_context
from kafka.errors import KafkaConnectionError
from unicorn_binance_websocket_api.manager import BinanceWebSocketApiManager
import asyncio
import json
import logging
import os
import time


async def import_last_price_to_kafka(kafka_config: dict = None,
                                     ubwa_manager: BinanceWebSocketApiManager = False,
                                     markets: list = None):

    kafka_producer = AIOKafkaProducer(bootstrap_servers=kafka_config['server'],
                                      security_protocol='SASL_SSL',
                                      sasl_mechanism='SCRAM-SHA-512',
                                      sasl_plain_username=kafka_config['user'],
                                      sasl_plain_password=kafka_config['pass'],
                                      ssl_context=create_ssl_context(),
                                      value_serializer=lambda x: json.dumps(x).encode("utf-8"))
    await kafka_producer.start()

    ubwa_manager.create_stream(channels="trade", markets=markets, output="UnicornFy")

    try:
        while True:
            data = ubwa_manager.pop_stream_data_from_stream_buffer()
            if data is not False:
                try:
                    event_type = data['event_type']
                    symbol = data['symbol']
                    trade_time = data['trade_time']
                    price = data['price']
                except KeyError:
                    continue
                if event_type == "trade":
                    topic = f"{kafka_config['user']}-{str(symbol).lower()}_binance_spot_last_trade_price"
                    message = {'symbol': symbol, 'time': trade_time, 'price': price}
                    print(topic, message)
                    await kafka_producer.send_and_wait(topic, message)
            else:
                time.sleep(0.01)
    finally:
        await kafka_producer.stop()
        ubwa_manager.stop_manager_with_all_streams()

if __name__ == "__main__":
    # Logging
    logging.basicConfig(level=logging.DEBUG,
                        filename=os.path.basename(__file__) + '.log',
                        format="{asctime} [{levelname:8}] {process} {thread} {module}: {message}",
                        style="{")

    # Config
    exchange = 'binance.com'
    symbols = ['ethusdt', 'btcusdt', 'ltcusdt']
    kafka = {"server": "glider.srvs.cloudkafka.com:9094",
             "user": "my-username", 
             "pass": "my-password"}
    
    # Get a license from https://shop.lucit.services/software/unicorn-binance-suite
    lucit_api_secret = "my-secret"
    lucit_license_token = "my-token"

    ubwa = BinanceWebSocketApiManager(exchange=exchange, 
                                      lucit_api_secret=lucit_api_secret, 
                                      lucit_license_token=lucit_license_token)

    try:
        asyncio.run(import_last_price_to_kafka(kafka_config=kafka, ubwa_manager=ubwa, markets=symbols))
    except KeyboardInterrupt:
        print("\r\nGracefully stopping ...")
    except KafkaConnectionError as error_msg:
        print(f"{error_msg}")

    ubwa.stop_manager_with_all_streams()
    
