import os
import sys
import json
import time
import threading
from functools import partial
from dotenv import load_dotenv

from websocket import WebSocketApp
from confluent_kafka import Producer

load_dotenv()

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

producer = Producer({
    'bootstrap.servers': os.getenv('KAFKA_BROKERS')
})

socket_urls = {
    'bybit-futures': "wss://stream.bybit.com/v5/public/linear",
    'bybit-spot': "wss://stream.bybit.com/v5/public/spot"
}

BYBIT_PAIRS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT",
    "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT",
    "UNIUSDT", "LTCUSDT", "ATOMUSDT", "NEARUSDT", "AAVEUSDT",
    "OPUSDT", "ARBUSDT", "SUIUSDT", "APTUSDT", "INJUSDT",
    "FILUSDT", "SEIUSDT", "TIAUSDT", "JUPUSDT", "WUSDT",
    "ENAUSDT", "TONUSDT", "WIFUSDT", "ONDOUSDT",
]


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
    if 'data' not in json_data:
        return
    data = json_data['data']
    if 'symbol' not in data:
        return

    symbol = data['symbol']

    last_price = float(data.get('lastPrice', 0)) if 'lastPrice' in data else None
    prev_price = float(data.get('prevPrice24h', 0)) if 'prevPrice24h' in data else None

    ticker = {}
    if last_price is not None:
        ticker['price'] = last_price
    if prev_price is not None and last_price is not None:
        ticker['change'] = last_price - prev_price
    if 'price24hPcnt' in data:
        ticker['changeP'] = float(data['price24hPcnt']) * 100
    if 'turnover24h' in data:
        ticker['vol_usd'] = float(data['turnover24h'])
    if 'volume24h' in data:
        ticker['vol_native'] = float(data['volume24h'])

    if ticker:
        send_to_kafka(exchange, symbol, ticker)
        producer.poll(0)


def send_ping(ws):
    while True:
        try:
            ws.send(json.dumps({"op": "ping"}))
            time.sleep(20)
        except Exception:
            break


def on_message(ws, message, exchange):
    try:
        json_data = json.loads(message)
        process_data(json_data, exchange)
    except Exception:
        logger.exception(f"{exchange.upper()} - Error processing message")


def on_error(ws, error):
    logger.error(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.info(f"WebSocket closed with status code {close_status_code}: {close_msg}")


def subscribe_to_assets(ws, assets, exchange):
    if not assets:
        return

    if exchange == 'bybit-futures':
        ws.send(json.dumps({
            "op": "subscribe",
            "args": [f"tickers.{asset}" for asset in assets]
        }))
    elif exchange == 'bybit-spot':
        batch_size = 10
        for i in range(0, len(assets), batch_size):
            batch = assets[i:i + batch_size]
            ws.send(json.dumps({
                "op": "subscribe",
                "args": [f"tickers.{asset}" for asset in batch]
            }))

    logger.info(f"{exchange.upper()} - Subscribed to {len(assets)} assets")


def on_open(ws, exchange):
    subscribe_to_assets(ws, BYBIT_PAIRS, exchange)
    logger.info(f"{exchange.upper()} - WebSocket opened successfully")

    ping_thread = threading.Thread(target=send_ping, args=(ws,))
    ping_thread.daemon = True
    ping_thread.start()


def receive_data(exchange):
    logger.info(f'{exchange.upper()} - Running receive_data...')
    socket_url = socket_urls.get(exchange)

    while True:
        try:
            ws = WebSocketApp(
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
    exchange = os.getenv('EXCHANGE', 'bybit-futures')
    run(exchange)
