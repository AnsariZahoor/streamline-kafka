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
]

HYPERLIQUID_HIP3_ASSETS = [
    # e.g. "vntl:SPACEX"
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
    initial_data = json_data['data']
    if 'coin' not in initial_data or 'ctx' not in initial_data:
        return

    symbol = initial_data['coin']
    pair_name = ASSETS_PAIR_MAP.get(exchange, {}).get(symbol)
    if not pair_name:
        return

    data = initial_data['ctx']

    ticker = {}
    mark_px = float(data['markPx']) if 'markPx' in data else None
    prev_day_px = float(data['prevDayPx']) if 'prevDayPx' in data else None

    if mark_px is not None:
        ticker['price'] = mark_px
    if mark_px is not None and prev_day_px is not None:
        ticker['change'] = mark_px - prev_day_px
        ticker['changeP'] = (ticker['change'] / prev_day_px * 100) if prev_day_px else 0.0
    if 'dayNtlVlm' in data:
        ticker['vol_usd'] = float(data['dayNtlVlm'])
    if 'dayBaseVlm' in data:
        ticker['vol_native'] = float(data['dayBaseVlm'])

    if ticker:
        send_to_kafka(exchange, pair_name, ticker)
        producer.poll(0)


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
    exchange = 'hyperliquid-futures'
    run(exchange)
