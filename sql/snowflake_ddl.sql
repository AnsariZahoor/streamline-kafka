CREATE TABLE IF NOT EXISTS SPREAD_ANALYTICS (
    symbol            VARCHAR(20)      NOT NULL,
    binance_price     FLOAT            NOT NULL,
    bybit_price       FLOAT            NOT NULL,
    spread_pct        FLOAT            NOT NULL,
    spread_mean_5m    FLOAT,
    spread_stddev_5m  FLOAT,
    spread_zscore     FLOAT,
    event_timestamp   TIMESTAMP_NTZ    NOT NULL,
    inserted_at       TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE VIEW SPREAD_ANALYTICS_LATEST AS
SELECT *
FROM SPREAD_ANALYTICS
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY event_timestamp DESC) = 1;
