"""
Example: how a serving endpoint would fetch features from the Feast online store (Redis).
Demonstrates get_online_features() for real-time inference.
"""

from pathlib import Path
from feast import FeatureStore

FEATURE_STORE_DIR = Path(__file__).parent.parent / "feature_store"


def main():
    store = FeatureStore(repo_path=str(FEATURE_STORE_DIR))

    features = store.get_online_features(
        features=[
            "ticker_features:price",
            "ticker_features:price_mean_5m",
            "ticker_features:price_std_5m",
            "ticker_features:price_change_pct",
            "ticker_features:volume_mean_5m",
            "ticker_features:tick_count_5m",
            "ticker_features:spread_pct",
        ],
        entity_rows=[
            {"symbol": "BTCUSDT", "exchange": "binance-futures"},
            {"symbol": "ETHUSDT", "exchange": "binance-futures"},
        ],
    ).to_dict()

    print("Online Features (from Redis):")
    for i, symbol in enumerate(features["symbol"]):
        print(f"\n  {symbol} @ {features['exchange'][i]}:")
        print(f"    price:            {features['price'][i]}")
        print(f"    price_mean_5m:    {features['price_mean_5m'][i]}")
        print(f"    price_std_5m:     {features['price_std_5m'][i]}")
        print(f"    price_change_pct: {features['price_change_pct'][i]}")
        print(f"    volume_mean_5m:   {features['volume_mean_5m'][i]}")
        print(f"    tick_count_5m:    {features['tick_count_5m'][i]}")
        print(f"    spread_pct:       {features['spread_pct'][i]}")


if __name__ == "__main__":
    main()
