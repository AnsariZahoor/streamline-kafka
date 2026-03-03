import os
import sys
import json
import time
import signal
import logging
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger('futures-arbitrage')

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092')
INPUT_TOPICS = ['binance-futures', 'bybit-futures']
OUTPUT_TOPIC = 'futures-arbitrage-signals'
CONSUMER_GROUP = 'futures-arbitrage-group'

THRESHOLD = float(os.getenv('ARBITRAGE_THRESHOLD', '0.2'))
STALENESS_MAX_S = int(os.getenv('STALENESS_SECONDS', '5'))
COMMIT_INTERVAL_S = 5
COMMIT_BATCH_SIZE = 1000

EXCHANGE_ALIASES = {
    'binance-futures': 'binance',
    'bybit-futures': 'bybit',
}
REQUIRED_EXCHANGES = set(EXCHANGE_ALIASES.values())


class ArbitrageDetector:
    def __init__(self):
        # {symbol: {exchange: {"price": float, "ts": int}}}
        self.prices: dict[str, dict[str, dict]] = {}
        # tracks active signal direction to suppress duplicates
        # {symbol: (buy_exchange, sell_exchange)}
        self.active_signals: dict[str, tuple[str, str]] = {}

        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKERS,
            'group.id': CONSUMER_GROUP,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
            'fetch.min.bytes': 1,
            'fetch.wait.max.ms': 100,
        })

        self.producer = Producer({
            'bootstrap.servers': KAFKA_BROKERS,
            'linger.ms': 5,
            'batch.num.messages': 100,
        })

        self.running = True
        self._msg_count = 0
        self._last_commit_ts = time.monotonic()

    def start(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        self.consumer.subscribe(INPUT_TOPICS)
        logger.info(
            f"Started | topics={INPUT_TOPICS} group={CONSUMER_GROUP} "
            f"threshold={THRESHOLD}% staleness={STALENESS_MAX_S}s"
        )

        try:
            while self.running:
                msg = self.consumer.poll(timeout=0.5)

                if msg is None:
                    self._maybe_commit()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Consumer error: {msg.error()}")
                    raise KafkaException(msg.error())

                self._process(msg)
                self._msg_count += 1
                self._maybe_commit()

        except KafkaException:
            logger.exception("Fatal Kafka error")
        finally:
            self._cleanup()

    # ── message processing ──────────────────────────────────────────────

    def _process(self, msg):
        try:
            data = json.loads(msg.value())
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Bad payload on {msg.topic()}: {e}")
            return

        try:
            symbol = data['symbol']
            price = float(data['price'])
            ts = int(data['timestamp'])
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Malformed message fields: {e}")
            return

        exchange = data.get('exchange', EXCHANGE_ALIASES.get(msg.topic()))
        if exchange not in REQUIRED_EXCHANGES:
            return

        if symbol not in self.prices:
            self.prices[symbol] = {}

        self.prices[symbol][exchange] = {'price': price, 'ts': ts}
        self._evaluate(symbol)

    # ── arbitrage evaluation ────────────────────────────────────────────

    def _evaluate(self, symbol):
        sym_data = self.prices[symbol]

        if len(sym_data) < 2:
            return

        binance = sym_data.get('binance')
        bybit = sym_data.get('bybit')
        if not binance or not bybit:
            return

        if abs(binance['ts'] - bybit['ts']) > STALENESS_MAX_S:
            return

        bp = binance['price']
        yp = bybit['price']
        min_price = min(bp, yp)
        if min_price <= 0:
            return

        spread_pct = abs(yp - bp) / min_price * 100

        if spread_pct > THRESHOLD:
            if bp < yp:
                buy_ex, sell_ex = 'binance', 'bybit'
                buy_px, sell_px = bp, yp
            else:
                buy_ex, sell_ex = 'bybit', 'binance'
                buy_px, sell_px = yp, bp

            prev = self.active_signals.get(symbol)
            if prev == (buy_ex, sell_ex):
                return

            self.active_signals[symbol] = (buy_ex, sell_ex)
            self._emit({
                'symbol': symbol,
                'buy_exchange': buy_ex,
                'sell_exchange': sell_ex,
                'buy_price': buy_px,
                'sell_price': sell_px,
                'spread_percent': round(spread_pct, 4),
                'timestamp': max(binance['ts'], bybit['ts']),
            })
        else:
            self.active_signals.pop(symbol, None)

    # ── signal emission ─────────────────────────────────────────────────

    def _emit(self, sig):
        self.producer.produce(
            topic=OUTPUT_TOPIC,
            key=sig['symbol'].encode(),
            value=json.dumps(sig).encode(),
            callback=self._on_delivery,
        )
        self.producer.poll(0)
        logger.info(
            f"ARB {sig['symbol']} "
            f"BUY {sig['buy_exchange']}@{sig['buy_price']} → "
            f"SELL {sig['sell_exchange']}@{sig['sell_price']} "
            f"spread={sig['spread_percent']}%"
        )

    @staticmethod
    def _on_delivery(err, msg):
        if err:
            logger.error(f"Signal delivery failed: {err}")

    # ── offset management ───────────────────────────────────────────────

    def _maybe_commit(self):
        now = time.monotonic()
        if (
            self._msg_count >= COMMIT_BATCH_SIZE
            or (self._msg_count > 0 and now - self._last_commit_ts >= COMMIT_INTERVAL_S)
        ):
            try:
                self.consumer.commit(asynchronous=False)
            except KafkaException as e:
                logger.warning(f"Offset commit failed: {e}")
            self._msg_count = 0
            self._last_commit_ts = now

    # ── lifecycle ───────────────────────────────────────────────────────

    def _handle_signal(self, signum, frame):
        logger.info(f"Signal {signum} received, shutting down...")
        self.running = False

    def _cleanup(self):
        logger.info("Flushing producer...")
        remaining = self.producer.flush(timeout=10)
        if remaining:
            logger.warning(f"{remaining} messages were not delivered")

        try:
            self.consumer.commit(asynchronous=False)
        except Exception:
            pass
        logger.info("Closing consumer...")
        self.consumer.close()
        logger.info("Shutdown complete")


if __name__ == '__main__':
    ArbitrageDetector().start()
