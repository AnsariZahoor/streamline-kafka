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

data_lock = threading.Lock()

producer = Producer({
    'bootstrap.servers': os.getenv('KAFKA_BROKERS')
})

socket_urls = {
    'hyperliquid-futures': "wss://api.hyperliquid.xyz/ws",
    'hyperliquid-spot': "wss://api.hyperliquid.xyz/ws",
    'hyperliquid-hip3': "wss://api.hyperliquid.xyz/ws"
}

HYPERLIQUID_FUTURES_ASSETS = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "DOT",
    "LINK", "UNI", "LTC", "ATOM", "NEAR", "AAVE", "OP", "ARB",
    "SUI", "APT", "INJ", "FIL", "SEI", "TIA", "JUP", "W",
    "ENA", "TON", "PEPE", "WIF", "ONDO",
]

HYPERLIQUID_SPOT_ASSETS = [
    # token_index -> pair_name, e.g. "@1": "PURR/USDC"
    # Fill in with your spot pairs
]

HYPERLIQUID_HIP3_ASSETS = [
    # e.g. "vntl:SPACEX"
    # Fill in with your hip3 pairs
]

ASSETS_PAIR_MAP = {
    'hyperliquid-futures': {asset: f"{asset}-USD" for asset in HYPERLIQUID_FUTURES_ASSETS},
    'hyperliquid-spot': {
        # "@1": "PURR/USDC",
        # "@2": "LICK/USDC",
    },
    'hyperliquid-hip3': {
        # "vntl:SPACEX": "vntl:SPACEX-USDH",
    },
}

ASSETS_BY_EXCHANGE = {
    'hyperliquid-futures': HYPERLIQUID_FUTURES_ASSETS,
    'hyperliquid-spot': HYPERLIQUID_SPOT_ASSETS,
    'hyperliquid-hip3': HYPERLIQUID_HIP3_ASSETS,
}

required_keys = ['markPx', 'prevDayPx', 'dayBaseVlm', 'dayNtlVlm']


class ExchangeData:
    def __init__(self):
        self.ticker_data = {}

exchange_data = ExchangeData()


def process_data(json_data, exchange):
    if 'data' not in json_data:
        return
    initial_data = json_data['data']
    if 'coin' not in initial_data or 'ctx' not in initial_data:
        return

    symbol = initial_data['coin']
    pair_name = ASSETS_PAIR_MAP.get(exchange, {}).get(symbol)
    if not pair_name:
        return

    data = initial_data['ctx']

    with data_lock:
        exchange_data.ticker_data.setdefault(exchange, {})
        exchange_data.ticker_data[exchange].setdefault(pair_name, {})

        exchange_data.ticker_data[exchange][pair_name].update(
            (key, data[key]) for key in required_keys if key in data
        )

        ticker = exchange_data.ticker_data[exchange][pair_name]
        if 'markPx' in ticker and 'prevDayPx' in ticker:
            mark_px = float(ticker['markPx'])
            prev_day_px = float(ticker['prevDayPx'])
            ticker['change'] = mark_px - prev_day_px
            ticker['changeP'] = (ticker['change'] / prev_day_px * 100) if prev_day_px else 0.0


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

    for asset in assets:
        ws.send(json.dumps({
            "method": "subscribe",
            "subscription": {"type": "activeAssetCtx", "coin": asset}
        }))

    logger.info(f"{exchange.upper()} - Subscribed to {len(assets)} assets")


def on_open(ws, exchange):
    assets = ASSETS_BY_EXCHANGE.get(exchange, [])
    subscribe_to_assets(ws, assets, exchange)
    logger.info(f"{exchange.upper()} - WebSocket opened successfully")


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


def upload_data_to_kafka(exchange):
    previous_data = {}
    logger.info(f'{exchange.upper()} - Running upload_data_to_kafka...')
    while True:
        try:
            with data_lock:
                raw = exchange_data.ticker_data.get(exchange)
                if not raw:
                    has_data = False
                else:
                    ticker_data_copy = {sym: dict(vals) for sym, vals in raw.items()}
                    has_data = True

            if not has_data:
                time.sleep(0.25)
                continue

            changed_data = []
            columns = ['price', 'change', 'changeP', 'vol_usd', 'vol_native']

            for token, data in ticker_data_copy.items():
                current_values = {
                    'price': data.get('markPx', 0),
                    'change': data.get('change', 0),
                    'changeP': float(data.get('changeP', 0)),
                    'vol_usd': data.get('dayNtlVlm', 0),
                    'vol_native': data.get('dayBaseVlm', 0),
                }

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
    exchange = 'hyperliquid-futures'
    run(exchange)
