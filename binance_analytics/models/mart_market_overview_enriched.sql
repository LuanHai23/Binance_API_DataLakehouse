{{ config(materialized='table') }}

-- Enriched market overview with SCD Type 2 symbol metadata
-- Use max candle timestamp as reference time instead of NOW()
-- This avoids empty marts when demo data is older than the database clock.

WITH data_watermark AS (
    SELECT
        MAX(candle_start_time) AS max_candle_time
    FROM {{ source('warehouse', 'fact_market_candles') }}
),

latest_candle AS (
    SELECT
        f.symbol,
        f.close_price,
        f.candle_start_time,
        ROW_NUMBER() OVER (
            PARTITION BY f.symbol
            ORDER BY f.candle_start_time DESC
        ) AS rn
    FROM {{ source('warehouse', 'fact_market_candles') }} f
),

first_candle_24h AS (
    SELECT
        f.symbol,
        f.open_price,
        f.candle_start_time,
        ROW_NUMBER() OVER (
            PARTITION BY f.symbol
            ORDER BY f.candle_start_time ASC
        ) AS rn
    FROM {{ source('warehouse', 'fact_market_candles') }} f
    CROSS JOIN data_watermark w
    WHERE f.candle_start_time >= w.max_candle_time - INTERVAL '24 hours'
),

stats_24h AS (
    SELECT
        f.symbol,
        MAX(f.high_price) AS high_24h,
        MIN(f.low_price) AS low_24h,
        SUM(f.total_volume) AS volume_24h,
        SUM(f.buy_volume_taker) AS buy_volume_24h,
        SUM(f.sell_volume_maker) AS sell_volume_24h,
        SUM(f.trade_count) AS total_trades_24h
    FROM {{ source('warehouse', 'fact_market_candles') }} f
    CROSS JOIN data_watermark w
    WHERE f.candle_start_time >= w.max_candle_time - INTERVAL '24 hours'
    GROUP BY f.symbol
),

current_symbol_dim AS (
    SELECT
        symbol,
        base_asset,
        quote_asset,
        status AS symbol_status,
        price_precision,
        quantity_precision,
        tick_size,
        step_size,
        min_qty,
        valid_from,
        valid_to,
        is_current
    FROM {{ source('warehouse', 'dim_symbol_scd') }}
    WHERE is_current = TRUE
)

SELECT
    s.symbol,
    d.base_asset,
    d.quote_asset,
    d.symbol_status,
    d.price_precision,
    d.quantity_precision,
    d.tick_size,
    d.step_size,
    d.min_qty,

    l.close_price AS current_price,
    f.open_price AS open_price_24h,
    s.high_24h,
    s.low_24h,

    ROUND(
        ((l.close_price - f.open_price) / NULLIF(f.open_price, 0) * 100)::numeric,
        2
    ) AS price_change_pct_24h,

    ROUND(
        (l.close_price - f.open_price)::numeric,
        8
    ) AS price_change_24h,

    ROUND(s.volume_24h::numeric, 8) AS volume_24h,
    ROUND(s.buy_volume_24h::numeric, 8) AS buy_volume_24h,
    ROUND(s.sell_volume_24h::numeric, 8) AS sell_volume_24h,

    ROUND(
        (s.buy_volume_24h / NULLIF(s.volume_24h, 0) * 100)::numeric,
        2
    ) AS buy_ratio_pct,

    s.total_trades_24h,
    l.candle_start_time AS last_updated,

    d.valid_from AS symbol_metadata_valid_from,
    d.valid_to AS symbol_metadata_valid_to,
    d.is_current AS symbol_metadata_is_current

FROM stats_24h s
LEFT JOIN latest_candle l
    ON s.symbol = l.symbol
    AND l.rn = 1
LEFT JOIN first_candle_24h f
    ON s.symbol = f.symbol
    AND f.rn = 1
LEFT JOIN current_symbol_dim d
    ON s.symbol = d.symbol