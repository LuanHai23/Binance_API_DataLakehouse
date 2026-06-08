{{ config(materialized='table') }}

WITH pressure AS (
    SELECT
        symbol,
        DATE_TRUNC('hour', candle_start_time) AS candle_hour,

        SUM(buy_volume_taker) AS buy_volume,
        SUM(sell_volume_maker) AS sell_volume,
        SUM(total_volume) AS total_volume,
        SUM(trade_count) AS total_trades,

        ROUND(
            (SUM(buy_volume_taker) / NULLIF(SUM(total_volume), 0) * 100)::numeric,
            2
        ) AS buy_pressure_pct,

        ROUND(
            (SUM(sell_volume_maker) / NULLIF(SUM(total_volume), 0) * 100)::numeric,
            2
        ) AS sell_pressure_pct

    FROM {{ source('warehouse', 'fact_market_candles') }}
    GROUP BY symbol, DATE_TRUNC('hour', candle_start_time)
)

SELECT
    *,
    CASE
        WHEN buy_pressure_pct >= 60 THEN 'Strong Buy Pressure'
        WHEN sell_pressure_pct >= 60 THEN 'Strong Sell Pressure'
        ELSE 'Neutral'
    END AS pressure_signal
FROM pressure
ORDER BY candle_hour DESC, symbol