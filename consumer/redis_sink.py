import os
import json
import time
import signal
import logging
import redis
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError, KafkaException

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger('redis-sink')

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
CONSUMER_GROUP = 'redis-sink-group'
INPUT_TOPICS = os.getenv(
    'KAFKA_TOPICS',
    'binance-futures-ticker,binance-spot-ticker,bybit-futures-ticker,bybit-spot-ticker,hyperliquid-futures-ticker'
).split(',')

KEY_PREFIX = os.getenv('REDIS_KEY_PREFIX', 'ticker')
KEY_TTL = int(os.getenv('REDIS_KEY_TTL', '60'))

COMMIT_INTERVAL_S = 5
COMMIT_BATCH_SIZE = 1000
PIPELINE_FLUSH_SIZE = 500


class RedisSink:
    """
    Consumes ticker updates from Kafka and writes them into Redis hashes.

    Redis key layout:
        ticker:{exchange}:{symbol}  → Hash  {price, change, changeP, vol_usd, vol_native}
        ticker:{exchange}:_symbols  → Set   {BTCUSDT, ETHUSDT, ...}

    Only changed fields are written per message (producer already does delta
    detection), so each HSET is minimal.  Writes are batched via pipeline.
    """

    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKERS,
            'group.id': CONSUMER_GROUP,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
            'fetch.min.bytes': 1,
            'fetch.wait.max.ms': 100,
        })

        self.rds = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        self._pipe = self.rds.pipeline(transaction=False)
        self._pipe_ops = 0

        self.running = True
        self._msg_count = 0
        self._last_commit_ts = time.monotonic()
        self._total_writes = 0

    # ── main loop ───────────────────────────────────────────────────────

    def start(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        self.rds.ping()
        self.consumer.subscribe(INPUT_TOPICS)
        logger.info(f"Started | topics={INPUT_TOPICS} redis={REDIS_URL} ttl={KEY_TTL}s")

        try:
            while self.running:
                msg = self.consumer.poll(timeout=0.5)

                if msg is None:
                    self._flush_pipeline()
                    self._maybe_commit()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Consumer error: {msg.error()}")
                    raise KafkaException(msg.error())

                self._process(msg)
                self._msg_count += 1

                if self._pipe_ops >= PIPELINE_FLUSH_SIZE:
                    self._flush_pipeline()
                self._maybe_commit()

        except KafkaException:
            logger.exception("Fatal Kafka error")
        except redis.RedisError:
            logger.exception("Fatal Redis error")
        finally:
            self._cleanup()

    # ── message processing ──────────────────────────────────────────────

    def _process(self, msg):
        try:
            data = json.loads(msg.value())
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Bad payload on {msg.topic()}: {e}")
            return

        exchange = data.get('exchange')
        token = data.get('token')
        fields = data.get('data')

        if not exchange or not token or not fields:
            return

        key = f"{KEY_PREFIX}:{exchange}:{token}"

        self._pipe.hset(key, mapping={k: str(v) for k, v in fields.items()})
        if KEY_TTL > 0:
            self._pipe.expire(key, KEY_TTL)
        self._pipe.sadd(f"{KEY_PREFIX}:{exchange}:_symbols", token)

        self._pipe_ops += 1

    # ── redis pipeline ──────────────────────────────────────────────────

    def _flush_pipeline(self):
        if self._pipe_ops == 0:
            return
        try:
            self._pipe.execute()
            self._total_writes += self._pipe_ops
            logger.debug(f"Flushed {self._pipe_ops} ops (total: {self._total_writes})")
        except redis.RedisError:
            logger.exception("Pipeline flush failed")
        self._pipe_ops = 0
        self._pipe = self.rds.pipeline(transaction=False)

    # ── offset management ───────────────────────────────────────────────

    def _maybe_commit(self):
        now = time.monotonic()
        if (
            self._msg_count >= COMMIT_BATCH_SIZE
            or (self._msg_count > 0 and now - self._last_commit_ts >= COMMIT_INTERVAL_S)
        ):
            self._flush_pipeline()
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
        self._flush_pipeline()
        try:
            self.consumer.commit(asynchronous=False)
        except Exception:
            pass
        logger.info("Closing consumer...")
        self.consumer.close()
        logger.info(f"Shutdown complete | total writes: {self._total_writes}")


if __name__ == '__main__':
    RedisSink().start()
