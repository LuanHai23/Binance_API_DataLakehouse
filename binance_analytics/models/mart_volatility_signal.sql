{{ config(materialized='table') }}

WITH volatility AS (
    SELECT
        symbol,
        DATE_TRUNC('hour', candle_start_time) AS candle_hour,

        MIN(low_price) AS hourly_low,
        MAX(high_price) AS hourly_high,
        AVG(close_price) AS avg_close_price,
        SUM(total_volume) AS total_volume,
        SUM(trade_count) AS total_trades,

        ROUND(
            ((MAX(high_price) - MIN(low_price)) / NULLIF(AVG(close_price), 0) * 100)::numeric,
            4
        ) AS price_range_pct

    FROM {{ source('warehouse', 'fact_market_candles') }}
    GROUP BY symbol, DATE_TRUNC('hour', candle_start_time)
)

SELECT
    *,
    CASE
        WHEN price_range_pct >= 5 THEN 'High Volatility'
        WHEN price_range_pct >= 2 THEN 'Medium Volatility'
        ELSE 'Low Volatility'
    END AS volatility_level
FROM volatility
ORDER BY candle_hour DESC, symbol