import os
import sys
import time
import json
import websocket
import threading
import pytz
from datetime import datetime
from functools import partial

from confluent_kafka import Producer

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

data_lock = threading.Lock()

producer = Producer({
    'bootstrap.servers': os.getenv('KAFKA_BROKERS')
})

class ExchangeData:
    def __init__(self):
        self.dfs = {}

exchange_data = ExchangeData()

def process_data(json_data, exchange):
    df = pd.DataFrame(json_data)[['s','c','p','P','q','v']]
    df = df[df['s'].str.endswith('USDT')]
    df = df.rename(columns={'c':'price', 'p':'change', 'P':'changeP','q':'vol_usd','v':'vol_native'})

    with data_lock:
        if exchange not in exchange_data.dfs:
            exchange_data.dfs[exchange] = []
        exchange_data.dfs[exchange].append(df)


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

def upload_data_to_kafka(exchange):
    previous_data = {}
    logger.info(f'{exchange.upper()} - Running upload_data_to_kafka...')
    while True:
        try:
            with data_lock:
                frames = exchange_data.dfs.get(exchange, [])
                if not frames:
                    has_data = False
                else:
                    snapshot = list(frames)
                    frames.clear()
                    has_data = True

            if not has_data:
                time.sleep(0.25)
                continue

            final_df = pd.concat(snapshot)
            final_df['e'] = exchange
            final_df['updated_at'] = datetime.now(pytz.utc)
            df_latest = final_df.drop_duplicates(subset='s', keep='last')

            changed_data = []
            columns = ['price', 'change', 'changeP', 'vol_usd', 'vol_native']

            for _, row in df_latest.iterrows():
                token = row['s']
                current_values = row[columns].to_dict()

                if token in previous_data:
                    changed_fields = {
                        field: float(current_values[field])
                        for field in columns
                        if field not in previous_data[token] or previous_data[token][field] != current_values[field]
                    }
                    if changed_fields:
                        changed_data.append((token, changed_fields))
                        previous_data[token].update(changed_fields)
                else:
                    changed_fields = {
                        field: float(current_values[field])
                        for field in columns
                    }
                    changed_data.append((token, changed_fields))
                    previous_data[token] = current_values

            if changed_data:
                logger.info(f'{exchange.upper()} - Updated to kafka: {len(changed_data)}')

                for token, changed_fields in changed_data:
                    producer.produce(
                        topic=f'{exchange}-ticker',
                        value=json.dumps({
                            'exchange': exchange,
                            'token': token,
                            'data': changed_fields
                        }),
                        callback=lambda err, msg: logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                                if err is None else logger.error(f"Message delivery failed: {err}")
                    )

                producer.flush()

        except Exception:
            logger.exception(f"{exchange.upper()} - Kafka upload error, retrying...")
            time.sleep(1)

def run(exchange):
    thread1 = threading.Thread(target=receive_data, args=(exchange,))
    thread2 = threading.Thread(target=upload_data_to_kafka, args=(exchange,))
    
    thread1.daemon = True
    thread2.daemon = True

    thread1.start()
    thread2.start()

    try:
        while True:
            time.sleep(1)
            if not thread1.is_alive() or not thread2.is_alive():
                logger.error("Critical thread died. Terminating program...")
                os._exit(1)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error in main thread: {e}")
        sys.exit(1)


if __name__ == '__main__':
    exchange = 'binance-futures'
    run(exchange)