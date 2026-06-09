INSERT INTO public.fact_market_candles (
    symbol,
    candle_start_time,
    candle_end_time,
    open_price,
    high_price,
    low_price,
    close_price,
    total_volume,
    quote_volume,
    trade_count,
    buy_volume_taker,
    sell_volume_maker,
    large_trade_count,
    gold_processed_at,
    loaded_at
)
SELECT
    symbol,
    candle_start_time,
    candle_end_time,
    open_price,
    high_price,
    low_price,
    close_price,
    total_volume,
    quote_volume,
    trade_count,
    buy_volume_taker,
    sell_volume_maker,
    large_trade_count,
    gold_processed_at,
    loaded_at
FROM public.stg_market_candles
ON CONFLICT (symbol, candle_start_time)
DO UPDATE SET
    candle_end_time = EXCLUDED.candle_end_time,
    open_price = EXCLUDED.open_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    close_price = EXCLUDED.close_price,
    total_volume = EXCLUDED.total_volume,
    quote_volume = EXCLUDED.quote_volume,
    trade_count = EXCLUDED.trade_count,
    buy_volume_taker = EXCLUDED.buy_volume_taker,
    sell_volume_maker = EXCLUDED.sell_volume_maker,
    large_trade_count = EXCLUDED.large_trade_count,
    gold_processed_at = EXCLUDED.gold_processed_at,
    loaded_at = EXCLUDED.loaded_at;