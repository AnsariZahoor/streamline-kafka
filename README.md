# Streamline Kafka — Real-Time Feature Pipeline for ML

A real-time data streaming pipeline that ingests live cryptocurrency ticker data from multiple exchanges via Kafka and serves ML-ready features through Feast.

![image](https://media.discordapp.net/attachments/1472256547722629382/1482202719883235451/image.png?ex=69b6185a&is=69b4c6da&hm=59d7a05f111601f8b9f0cbe39b702e17e71f4e997a3efa4ef995612cbb64e670&=&format=webp&quality=lossless&width=887&height=562)

## Why Kafka?

Batch-processing features every hour means your ML model sees stale data. Kafka enables real-time streaming where multiple consumers independently read the same data stream for different purposes — feature computation, live serving, alerting — all decoupled.

## Why Feast?

Without a feature store, training and serving features are computed in different code paths. Even small differences cause **training-serving skew** — the model silently degrades in production. Feast solves this:

| | Store | Feast API | Used By |
|---|---|---|---|
| **Training** | S3/MinIO (Parquet) | `get_historical_features()` | Data Scientists |
| **Serving** | Redis | `get_online_features()` | ML Engineers |

Same feature definitions. Same computation. Zero skew.

## Project Structure

```
├── producer/
│   ├── ws_binance.py          # Binance futures/spot WebSocket → Kafka
│   ├── ws_bybit.py            # Bybit futures/spot WebSocket → Kafka
│   └── ws_hyperliquid.py      # Hyperliquid WebSocket → Kafka
├── consumer/
│   ├── feature_sink.py        # Kafka → rolling features → Parquet on S3/MinIO
│   ├── redis_sink.py          # Kafka → Redis (live ticker state)
│   ├── arb_detector.py        # Kafka → cross-exchange spread detection → Kafka
│   └── snowflake_sink.py      # Kafka → Snowflake (historical storage)
├── feature_store/
│   ├── feature_store.yaml     # Feast config (offline: S3, online: Redis)
│   └── features.py            # Feature definitions (ticker_features)
├── scripts/
│   ├── sync_and_materialize.py  # S3 → local sync + feast materialize to Redis
│   ├── example_training.py      # Pull historical features for training
│   └── example_serving.py       # Fetch online features for inference
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Features Computed

Per symbol, per exchange, over a 5-minute rolling window:

| Feature | Description |
|---|---|
| `price` | Latest price |
| `price_mean_5m` | Rolling 5-min mean price |
| `price_std_5m` | Rolling 5-min price standard deviation |
| `price_change_pct` | % change from oldest to newest in window |
| `volume_usd` | Latest volume in USD |
| `volume_mean_5m` | Rolling 5-min mean volume |
| `tick_count_5m` | Number of ticks in window |
| `spread_pct` | Bid/ask proxy spread |

## Quick Start

### 1. Start infrastructure

```bash
docker compose up -d
```

This starts Kafka, Redis Stack, MinIO, Redpanda Console, producers, and consumers.

### 2. Verify data flow

- **Redpanda Console**: http://localhost:8080 — browse Kafka topics and messages
- **Redis Insight**: http://localhost:8001 — inspect live ticker data in Redis
- **MinIO Console**: http://localhost:9001 — view Parquet files in the feature store bucket

### 3. Sync features and materialize to Redis

```bash
python scripts/sync_and_materialize.py --apply
```

This syncs Parquet files from MinIO, runs `feast apply`, and materializes features into Redis.

### 4. Pull features for training (Data Scientist)

```python
from feast import FeatureStore

store = FeatureStore(repo_path="feature_store")

training_df = store.get_historical_features(
    entity_df=entity_df,  # DataFrame with symbol, exchange, event_timestamp
    features=[
        "ticker_features:price",
        "ticker_features:price_mean_5m",
        "ticker_features:price_std_5m",
        "ticker_features:price_change_pct",
    ],
).to_df()
```

### 5. Fetch features for inference (ML Engineer)

```python
from feast import FeatureStore

store = FeatureStore(repo_path="feature_store")

features = store.get_online_features(
    features=[
        "ticker_features:price",
        "ticker_features:price_mean_5m",
        "ticker_features:price_std_5m",
    ],
    entity_rows=[
        {"symbol": "BTCUSDT", "exchange": "binance-futures"},
    ],
).to_dict()
```

## Services

| Service | Port | Description |
|---|---|---|
| Kafka | 9092 | Message broker (KRaft mode) |
| Redpanda Console | 8080 | Kafka UI |
| Redis Stack | 6379 / 8001 | Online store + RedisInsight UI |
| MinIO | 9000 / 9001 | S3-compatible offline store + console |

## Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
KAFKA_BROKERS=localhost:9092
```

See `.env.example` for all available configuration options.
