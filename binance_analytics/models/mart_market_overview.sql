{{ config(materialized='table') }}

WITH watermark AS (
    SELECT
        MAX(candle_start_time) AS max_candle_time
    FROM {{ source('warehouse', 'fact_market_candles') }}
),

base AS (
    SELECT
        f.*
    FROM {{ source('warehouse', 'fact_market_candles') }} f
    CROSS JOIN watermark w
    WHERE f.candle_start_time >= w.max_candle_time - INTERVAL '24 hours'
),

aggregated AS (
    SELECT
        symbol,
        MAX(candle_start_time) AS last_updated,
        SUM(total_volume) AS volume_24h,
        SUM(quote_volume) AS quote_volume_24h,
        SUM(trade_count) AS trade_count_24h,
        SUM(buy_volume_taker) AS buy_volume_24h,
        SUM(sell_volume_maker) AS sell_volume_24h
    FROM base
    GROUP BY symbol
),

latest_price AS (
    SELECT DISTINCT ON (symbol)
        symbol,
        close_price AS current_price
    FROM base
    ORDER BY symbol, candle_start_time DESC
),

first_price AS (
    SELECT DISTINCT ON (symbol)
        symbol,
        open_price AS first_price_24h
    FROM base
    ORDER BY symbol, candle_start_time ASC
)

SELECT
    a.symbol,
    lp.current_price,
    fp.first_price_24h,

    ROUND(
        ((lp.current_price - fp.first_price_24h) / NULLIF(fp.first_price_24h, 0) * 100)::numeric,
        4
    ) AS price_change_pct_24h,

    a.volume_24h,
    a.quote_volume_24h,
    a.trade_count_24h,

    ROUND(
        (a.buy_volume_24h / NULLIF(a.volume_24h, 0) * 100)::numeric,
        2
    ) AS buy_ratio_pct,

    a.last_updated

FROM aggregated a
JOIN latest_price lp
    ON a.symbol = lp.symbol
JOIN first_price fp
    ON a.symbol = fp.symbol
ORDER BY a.quote_volume_24h DESC