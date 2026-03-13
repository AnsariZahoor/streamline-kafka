import os
import sys
import time
import json
import websocket
import threading
from functools import partial

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

producer = Producer({
    'bootstrap.servers': os.getenv('KAFKA_BROKERS')
})

columns_map = {'c': 'price', 'p': 'change', 'P': 'changeP', 'q': 'vol_usd', 'v': 'vol_native'}


def send_to_kafka(exchange, token, data):
    producer.produce(
        topic=f'{exchange}-ticker',
        value=json.dumps({
            'exchange': exchange,
            'token': token,
            'data': data
        }),
        callback=lambda err, msg: logger.error(f"Message delivery failed: {err}") if err else None
    )


def process_data(json_data, exchange):
    count = 0
    for item in json_data:
        symbol = item.get('s', '')
        if not symbol.endswith('USDT'):
            continue

        data = {
            columns_map[k]: float(item[k])
            for k in columns_map
            if k in item
        }

        send_to_kafka(exchange, symbol, data)
        count += 1

    producer.flush()
    if count:
        logger.info(f'{exchange.upper()} - Pushed {count} tickers to kafka')


def on_message(ws, message, exchange):
    try:
        json_data = json.loads(message)
        process_data(json_data, exchange)
    except Exception:
        logger.exception(f"{exchange.upper()} - Error processing message")


def on_error(ws, error):
    logger.info(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.info(f"WebSocket closed with status code {close_status_code}: {close_msg}")

def on_open(ws, exchange):
    logger.info(f"{exchange.upper()} - WebSocket opened successfully")


def receive_data(exchange):
    logger.info(f'{exchange.upper()} - Running receive_data...')
    socket_urls = {
        'binance-futures': "wss://fstream.binance.com/ws/!ticker@arr",
        'binance-spot': "wss://stream.binance.com:9443/ws/!ticker@arr"
    }
    socket_url = socket_urls.get(exchange)

    while True:
        try:
            ws = websocket.WebSocketApp(
                socket_url,
                on_message=lambda ws, message: on_message(ws, message, exchange),
                on_error=on_error,
                on_close=on_close,
            )
            ws.on_open = partial(on_open, exchange=exchange)
            ws.run_forever()
        except Exception:
            logger.exception(f"{exchange.upper()} - WS error, reconnecting in 5s...")
        time.sleep(5)


def run(exchange):
    thread = threading.Thread(target=receive_data, args=(exchange,))
    thread.daemon = True
    thread.start()

    try:
        while True:
            time.sleep(1)
            if not thread.is_alive():
                logger.error("Critical thread died. Terminating program...")
                os._exit(1)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error in main thread: {e}")
        sys.exit(1)


if __name__ == '__main__':
    exchange = os.getenv('EXCHANGE', 'binance-futures')
    run(exchange)
