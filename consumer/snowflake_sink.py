import os
import json
import time
import signal
import logging
from datetime import datetime, timezone

import snowflake.connector
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError, KafkaException

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger('snowflake-sink')

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092')
INPUT_TOPIC = os.getenv('SNOWFLAKE_KAFKA_TOPIC', 'spread-detector-signals')
CONSUMER_GROUP = 'snowflake-sink-group'

SNOWFLAKE_HOST = os.getenv('SNOWFLAKE_HOST')
SNOWFLAKE_PORT = int(os.getenv('SNOWFLAKE_PORT', '4566'))
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT', 'test')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER', 'test')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD', 'test')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'test')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'test')
SNOWFLAKE_TABLE = os.getenv('SNOWFLAKE_TABLE', 'SPREAD_ANALYTICS')

BATCH_SIZE = int(os.getenv('SNOWFLAKE_BATCH_SIZE', '1000'))
FLUSH_INTERVAL_S = int(os.getenv('SNOWFLAKE_FLUSH_INTERVAL', '10'))

INSERT_SQL = f"""
    INSERT INTO {SNOWFLAKE_TABLE}
        (symbol, binance_price, bybit_price, spread_pct,
         spread_mean_5m, spread_stddev_5m, spread_zscore, event_timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


class SnowflakeSink:

    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKERS,
            'group.id': CONSUMER_GROUP,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
        })

        self.conn = None
        self.batch: list[tuple] = []
        self.running = True
        self._msg_count = 0
        self._last_flush_ts = time.monotonic()
        self._total_rows = 0

    # ── snowflake connection ────────────────────────────────────────────

    def _connect(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

        connect_kwargs = {
            'account': SNOWFLAKE_ACCOUNT,
            'user': SNOWFLAKE_USER,
            'password': SNOWFLAKE_PASSWORD,
            'database': SNOWFLAKE_DATABASE,
            'schema': SNOWFLAKE_SCHEMA,
            'warehouse': SNOWFLAKE_WAREHOUSE,
        }
        if SNOWFLAKE_HOST:
            connect_kwargs['host'] = SNOWFLAKE_HOST
            connect_kwargs['port'] = SNOWFLAKE_PORT

        self.conn = snowflake.connector.connect(**connect_kwargs)
        logger.info(f"Connected to Snowflake {SNOWFLAKE_HOST or SNOWFLAKE_ACCOUNT} / {SNOWFLAKE_DATABASE}")

    # ── main loop ───────────────────────────────────────────────────────

    def start(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        self._connect()
        self.consumer.subscribe([INPUT_TOPIC])
        logger.info(
            f"Started | topic={INPUT_TOPIC} table={SNOWFLAKE_TABLE} "
            f"batch={BATCH_SIZE} flush_interval={FLUSH_INTERVAL_S}s"
        )

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    self._maybe_flush()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Consumer error: {msg.error()}")
                    raise KafkaException(msg.error())

                self._process(msg)
                self._msg_count += 1
                self._maybe_flush()

        except KafkaException:
            logger.exception("Fatal Kafka error")
        finally:
            self._cleanup()

    # ── message processing ──────────────────────────────────────────────

    def _process(self, msg):
        try:
            data = json.loads(msg.value())
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Bad payload: {e}")
            return

        try:
            event_ts = datetime.fromtimestamp(
                data['event_timestamp'] / 1000, tz=timezone.utc
            ).strftime('%Y-%m-%d %H:%M:%S.%f')

            row = (
                data['symbol'],
                data['binance_price'],
                data['bybit_price'],
                data['spread_pct'],
                data['spread_mean_5m'],
                data['spread_stddev_5m'],
                data['spread_zscore'],
                event_ts,
            )
            self.batch.append(row)
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Malformed spread-analytics message: {e}")

    # ── batch flush ─────────────────────────────────────────────────────

    def _maybe_flush(self):
        now = time.monotonic()
        if (
            len(self.batch) >= BATCH_SIZE
            or (self.batch and now - self._last_flush_ts >= FLUSH_INTERVAL_S)
        ):
            self._flush()

    def _flush(self):
        if not self.batch:
            return

        cursor = self.conn.cursor()
        try:
            cursor.executemany(INSERT_SQL, self.batch)
            self._total_rows += len(self.batch)
            logger.info(f"Flushed {len(self.batch)} rows (total: {self._total_rows})")
            self.batch.clear()
            self._last_flush_ts = time.monotonic()

            try:
                self.consumer.commit(asynchronous=False)
            except KafkaException as e:
                logger.warning(f"Offset commit failed: {e}")

        except snowflake.connector.errors.ProgrammingError:
            logger.exception("Snowflake write failed, reconnecting...")
            self._connect()
        except snowflake.connector.errors.DatabaseError:
            logger.exception("Snowflake connection lost, reconnecting...")
            self._connect()
        finally:
            cursor.close()

    # ── lifecycle ───────────────────────────────────────────────────────

    def _handle_signal(self, signum, frame):
        logger.info(f"Signal {signum} received, shutting down...")
        self.running = False

    def _cleanup(self):
        self._flush()
        logger.info("Closing consumer...")
        self.consumer.close()
        if self.conn:
            self.conn.close()
        logger.info(f"Shutdown complete | total rows written: {self._total_rows}")


if __name__ == '__main__':
    SnowflakeSink().start()
