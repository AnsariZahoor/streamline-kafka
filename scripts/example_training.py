"""
Example: how a training job would pull features from the Feast offline store.
Demonstrates get_historical_features() with point-in-time joins.
"""

import pandas as pd
from pathlib import Path
from feast import FeatureStore

FEATURE_STORE_DIR = Path(__file__).parent.parent / "feature_store"


def main():
    store = FeatureStore(repo_path=str(FEATURE_STORE_DIR))

    entity_df = pd.DataFrame({
        "symbol": ["BTCUSDT", "ETHUSDT", "BTCUSDT", "ETHUSDT"],
        "exchange": [
            "binance-futures", "binance-futures",
            "bybit-futures", "bybit-futures",
        ],
        "event_timestamp": pd.to_datetime([
            "2026-03-12 10:00:00+00:00",
            "2026-03-12 10:00:00+00:00",
            "2026-03-12 10:00:00+00:00",
            "2026-03-12 10:00:00+00:00",
        ]),
    })

    training_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "ticker_features:price",
            "ticker_features:price_mean_5m",
            "ticker_features:price_std_5m",
            "ticker_features:price_change_pct",
            "ticker_features:volume_usd",
            "ticker_features:volume_mean_5m",
            "ticker_features:tick_count_5m",
            "ticker_features:spread_pct",
        ],
    ).to_df()

    print("Training DataFrame:")
    print(training_df)
    print(f"\nShape: {training_df.shape}")
    print(f"Columns: {list(training_df.columns)}")


if __name__ == "__main__":
    main()
