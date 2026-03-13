from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float64, Int64, String


ticker_entity = Entity(
    name="ticker",
    join_keys=["symbol", "exchange"],
    description="A crypto trading pair on a specific exchange",
)

ticker_feature_source = FileSource(
    path="s3://feature-store/ticker_features/",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    s3_endpoint_override="${S3_ENDPOINT_URL:http://localhost:9000}",
)

ticker_features = FeatureView(
    name="ticker_features",
    entities=[ticker_entity],
    ttl=timedelta(minutes=10),
    schema=[
        Field(name="price", dtype=Float64),
        Field(name="price_mean_5m", dtype=Float64),
        Field(name="price_std_5m", dtype=Float64),
        Field(name="price_change_pct", dtype=Float64),
        Field(name="volume_usd", dtype=Float64),
        Field(name="volume_mean_5m", dtype=Float64),
        Field(name="tick_count_5m", dtype=Int64),
        Field(name="spread_pct", dtype=Float64),
    ],
    source=ticker_feature_source,
    online=True,
)
