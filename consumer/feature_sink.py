"""
Consumes ticker updates from Kafka, computes rolling features per symbol,
and writes feature batches as Parquet files to S3/MinIO (Feast offline store).

Features computed per symbol:
  - price_mean_5m:    rolling 5-min mean price
  - price_std_5m:     rolling 5-min price std deviation
  - price_change_pct: % change from oldest to newest in window
  - volume_mean_5m:   rolling 5-min mean volume (USD)
  - tick_count_5m:    number of ticks in window
  - spread_pct:       latest bid/ask proxy spread (change / price)
"""

import os
import io
import math
import json
import time
import signal
import logging
from datetime import datetime, timezone
from collections import defaultdict

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError, KafkaException

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
)
logger = logging.getLogger("feature-sink")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
CONSUMER_GROUP = "feature-sink-group"
INPUT_TOPICS = os.getenv(
    "FEATURE_KAFKA_TOPICS", "binance-futures-ticker,bybit-futures-ticker"
).split(",")

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
S3_BUCKET = os.getenv("FEATURE_S3_BUCKET", "feature-store")
S3_PREFIX = os.getenv("FEATURE_S3_PREFIX", "ticker_features")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

WINDOW_SECONDS = int(os.getenv("FEATURE_WINDOW_SECONDS", "300"))
FLUSH_INTERVAL_S = int(os.getenv("FEATURE_FLUSH_INTERVAL", "60"))
COMMIT_INTERVAL_S = 5

ARROW_SCHEMA = pa.schema([
    ("symbol", pa.string()),
    ("exchange", pa.string()),
    ("event_timestamp", pa.timestamp("us", tz="UTC")),
    ("created_timestamp", pa.timestamp("us", tz="UTC")),
    ("price", pa.float64()),
    ("price_mean_5m", pa.float64()),
    ("price_std_5m", pa.float64()),
    ("price_change_pct", pa.float64()),
    ("volume_usd", pa.float64()),
    ("volume_mean_5m", pa.float64()),
    ("tick_count_5m", pa.int64()),
    ("spread_pct", pa.float64()),
])


class FeatureComputer:
    """Maintains a rolling window buffer per (symbol, exchange) and computes features."""

    def __init__(self, window_seconds: int = WINDOW_SECONDS):
        self.window_seconds = window_seconds
        self.buffers: dict[str, list[dict]] = defaultdict(list)

    def _key(self, symbol: str, exchange: str) -> str:
        return f"{exchange}:{symbol}"

    def add_tick(self, symbol: str, exchange: str, price: float, volume: float, ts: float):
        key = self._key(symbol, exchange)
        self.buffers[key].append({"ts": ts, "price": price, "volume": volume})
        cutoff = ts - self.window_seconds
        self.buffers[key] = [t for t in self.buffers[key] if t["ts"] >= cutoff]

    def compute(self, symbol: str, exchange: str, price: float, volume: float) -> dict | None:
        key = self._key(symbol, exchange)
        buf = self.buffers.get(key, [])
        if len(buf) < 2:
            return None

        prices = [t["price"] for t in buf]
        volumes = [t["volume"] for t in buf]
        n = len(prices)
        mean_p = sum(prices) / n
        mean_v = sum(volumes) / n

        variance = sum((p - mean_p) ** 2 for p in prices) / (n - 1)
        std_p = math.sqrt(variance)
        change_pct = ((prices[-1] - prices[0]) / prices[0] * 100) if prices[0] > 0 else 0.0
        spread_pct = abs(price * 0.0001) if price > 0 else 0.0

        return {
            "price_mean_5m": round(mean_p, 6),
            "price_std_5m": round(std_p, 6),
            "price_change_pct": round(change_pct, 6),
            "volume_mean_5m": round(mean_v, 4),
            "tick_count_5m": n,
            "spread_pct": round(spread_pct, 6),
        }


class FeatureSink:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "fetch.min.bytes": 1,
            "fetch.wait.max.ms": 100,
        })

        self.s3 = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name="us-east-1",
        )

        self.computer = FeatureComputer(WINDOW_SECONDS)
        self.feature_batch: list[dict] = []
        self.running = True
        self._msg_count = 0
        self._last_commit_ts = time.monotonic()
        self._last_flush_ts = time.monotonic()
        self._total_writes = 0

    def start(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        self._ensure_bucket()
        self.consumer.subscribe(INPUT_TOPICS)
        logger.info(
            f"Started | topics={INPUT_TOPICS} bucket={S3_BUCKET} "
            f"window={WINDOW_SECONDS}s flush_interval={FLUSH_INTERVAL_S}s"
        )

        try:
            while self.running:
                msg = self.consumer.poll(timeout=0.5)

                if msg is None:
                    self._maybe_flush()
                    self._maybe_commit()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Consumer error: {msg.error()}")
                    raise KafkaException(msg.error())

                self._process(msg)
                self._msg_count += 1
                self._maybe_flush()
                self._maybe_commit()

        except KafkaException:
            logger.exception("Fatal Kafka error")
        except Exception:
            logger.exception("Fatal error")
        finally:
            self._cleanup()

    def _process(self, msg):
        try:
            data = json.loads(msg.value())
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Bad payload on {msg.topic()}: {e}")
            return

        exchange = data.get("exchange", "")
        symbol = data.get("token", "")
        fields = data.get("data", {})

        price = fields.get("price")
        vol_usd = fields.get("vol_usd", 0.0)
        if not exchange or not symbol or price is None:
            return

        price = float(price)
        vol_usd = float(vol_usd)
        now = time.time()

        self.computer.add_tick(symbol, exchange, price, vol_usd, now)
        features = self.computer.compute(symbol, exchange, price, vol_usd)
        if features is None:
            return

        now_dt = datetime.now(timezone.utc)
        self.feature_batch.append({
            "symbol": symbol,
            "exchange": exchange,
            "event_timestamp": now_dt,
            "created_timestamp": now_dt,
            "price": price,
            "volume_usd": vol_usd,
            **features,
        })

    def _maybe_flush(self):
        now = time.monotonic()
        if now - self._last_flush_ts < FLUSH_INTERVAL_S or not self.feature_batch:
            return
        self._flush_to_s3()

    def _flush_to_s3(self):
        if not self.feature_batch:
            return

        table = pa.Table.from_pylist(self.feature_batch, schema=ARROW_SCHEMA)

        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        ts = datetime.now(timezone.utc)
        key = (
            f"{S3_PREFIX}/"
            f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/"
            f"{ts.strftime('%H%M%S')}_{len(self.feature_batch)}.parquet"
        )

        self.s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
        self._total_writes += len(self.feature_batch)
        logger.info(f"Flushed {len(self.feature_batch)} rows to s3://{S3_BUCKET}/{key}")

        self.feature_batch.clear()
        self._last_flush_ts = time.monotonic()

    def _maybe_commit(self):
        now = time.monotonic()
        if (
            self._msg_count >= 1000
            or (self._msg_count > 0 and now - self._last_commit_ts >= COMMIT_INTERVAL_S)
        ):
            try:
                self.consumer.commit(asynchronous=False)
            except KafkaException as e:
                logger.warning(f"Offset commit failed: {e}")
            self._msg_count = 0
            self._last_commit_ts = now

    def _ensure_bucket(self):
        try:
            self.s3.head_bucket(Bucket=S3_BUCKET)
        except Exception:
            self.s3.create_bucket(Bucket=S3_BUCKET)
            logger.info(f"Created bucket: {S3_BUCKET}")

    def _handle_signal(self, signum, frame):
        logger.info(f"Signal {signum} received, shutting down...")
        self.running = False

    def _cleanup(self):
        self._flush_to_s3()
        try:
            self.consumer.commit(asynchronous=False)
        except Exception:
            pass
        self.consumer.close()
        logger.info(f"Shutdown complete | total writes: {self._total_writes}")


if __name__ == "__main__":
    FeatureSink().start()
