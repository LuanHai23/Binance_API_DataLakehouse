CREATE TABLE IF NOT EXISTS public.stg_market_candles (
    symbol VARCHAR(20) NOT NULL,
    candle_start_time TIMESTAMP NOT NULL,
    candle_end_time TIMESTAMP NOT NULL,

    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,

    total_volume DOUBLE PRECISION,
    quote_volume DOUBLE PRECISION,
    trade_count BIGINT,

    buy_volume_taker DOUBLE PRECISION,
    sell_volume_maker DOUBLE PRECISION,
    large_trade_count BIGINT,

    gold_processed_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);