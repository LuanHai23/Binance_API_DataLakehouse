{{ config(materialized='table') }}

WITH base AS (
    SELECT
        symbol,
        COUNT(*) AS candle_count,
        SUM(trade_count) AS total_trades,
        SUM(total_volume) AS total_base_volume,
        SUM(quote_volume) AS total_quote_volume,
        AVG(trade_count) AS avg_trades_per_candle,
        AVG(total_volume) AS avg_volume_per_candle,
        MAX(candle_start_time) AS latest_candle_time
    FROM {{ source('warehouse', 'fact_market_candles') }}
    GROUP BY symbol
),

ranked AS (
    SELECT
        *,
        RANK() OVER (ORDER BY total_quote_volume DESC) AS liquidity_rank,

        CASE
            WHEN total_quote_volume >= 100000000 THEN 'Very High'
            WHEN total_quote_volume >= 10000000 THEN 'High'
            WHEN total_quote_volume >= 1000000 THEN 'Medium'
            ELSE 'Low'
        END AS liquidity_level
    FROM base
)

SELECT *
FROM ranked
ORDER BY liquidity_rank