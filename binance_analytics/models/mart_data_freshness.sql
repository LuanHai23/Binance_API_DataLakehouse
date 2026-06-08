{{ config(materialized='table') }}

-- Data freshness monitoring by symbol
-- Uses latest candle time in fact_market_candles to detect stale/dead symbols

WITH expected_symbols AS (
    SELECT 'BTCUSDT' AS symbol UNION ALL
    SELECT 'ETHUSDT' UNION ALL
    SELECT 'BNBUSDT' UNION ALL
    SELECT 'SOLUSDT' UNION ALL
    SELECT 'XRPUSDT' UNION ALL
    SELECT 'ADAUSDT' UNION ALL
    SELECT 'DOGEUSDT' UNION ALL
    SELECT 'SHIBUSDT'
),

symbol_freshness AS (
    SELECT
        symbol,
        MAX(candle_start_time) AS latest_candle_time,
        MIN(candle_start_time) AS earliest_candle_time,
        COUNT(*) AS total_candles,
        SUM(trade_count) AS total_trades,
        SUM(total_volume) AS total_volume
    FROM {{ source('warehouse', 'fact_market_candles') }}
    GROUP BY symbol
),

global_watermark AS (
    SELECT
        MAX(candle_start_time) AS global_latest_candle_time
    FROM {{ source('warehouse', 'fact_market_candles') }}
),

freshness AS (
    SELECT
        e.symbol,

        s.earliest_candle_time,
        s.latest_candle_time,
        g.global_latest_candle_time,

        COALESCE(s.total_candles, 0) AS total_candles,
        COALESCE(s.total_trades, 0) AS total_trades,
        COALESCE(s.total_volume, 0) AS total_volume,

        ROUND(
            (
                EXTRACT(EPOCH FROM (g.global_latest_candle_time - s.latest_candle_time)) / 60
            )::numeric,
            2
        ) AS minutes_behind_latest_symbol,

        CASE
            WHEN s.symbol IS NULL THEN 'Missing'
            WHEN EXTRACT(EPOCH FROM (g.global_latest_candle_time - s.latest_candle_time)) / 60 <= 5
                THEN 'Fresh'
            WHEN EXTRACT(EPOCH FROM (g.global_latest_candle_time - s.latest_candle_time)) / 60 <= 30
                THEN 'Stale'
            ELSE 'Dead'
        END AS freshness_status,

        CURRENT_TIMESTAMP AS checked_at

    FROM expected_symbols e
    LEFT JOIN symbol_freshness s
        ON e.symbol = s.symbol
    CROSS JOIN global_watermark g
)

SELECT *
FROM freshness
ORDER BY
    CASE freshness_status
        WHEN 'Missing' THEN 1
        WHEN 'Dead' THEN 2
        WHEN 'Stale' THEN 3
        WHEN 'Fresh' THEN 4
        ELSE 5
    END,
    symbol