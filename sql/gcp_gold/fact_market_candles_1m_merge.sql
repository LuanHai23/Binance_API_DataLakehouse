-- Cloud Gold v1: deterministic, guarded, idempotent one-minute candle load.
-- Required named parameters:
--   @source_batch  STRING
--   @event_date    DATE
--   @allow_mutation BOOL

DECLARE run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
DECLARE expected_symbols_csv STRING DEFAULT
  'ADAUSDT,BNBUSDT,BTCUSDT,DOGEUSDT,ETHUSDT,SHIBUSDT,SOLUSDT,XRPUSDT';
DECLARE source_row_count INT64 DEFAULT 0;
DECLARE eligible_row_count INT64 DEFAULT 0;

ASSERT
  @allow_mutation IS NOT NULL
AS 'allow_mutation must be explicitly true or false';

ASSERT
  REGEXP_CONTAINS(
    @source_batch,
    r'^[0-9]{8}t([01][0-9]|2[0-3])z$'
  )
AS 'source_batch must match YYYYMMDDtHHz with a valid UTC hour';

ASSERT
  SAFE.PARSE_DATE(
    '%Y%m%d',
    SUBSTR(@source_batch, 1, 8)
  ) = @event_date
AS 'event_date must match the date encoded in source_batch';

CREATE TEMP TABLE source_rows AS
SELECT
  symbol,
  event_id,
  trade_id,
  price,
  quantity,
  trade_value,
  event_time,
  trade_side,
  source_batch,
  event_date
FROM `binance-lakehouse-dev-1611.binance_silver_dev.aggtrade`
WHERE source_batch = @source_batch
  AND event_date = @event_date;

SET source_row_count = (
  SELECT COUNT(*)
  FROM source_rows
);

CREATE TEMP TABLE eligible_rows AS
SELECT
  *,
  TIMESTAMP_TRUNC(event_time, MINUTE) AS candle_start_time
FROM source_rows
WHERE symbol IS NOT NULL
  AND event_id IS NOT NULL
  AND event_time IS NOT NULL
  AND price IS NOT NULL
  AND price > NUMERIC '0'
  AND quantity IS NOT NULL
  AND quantity > NUMERIC '0'
  AND trade_value IS NOT NULL
  AND trade_value >= NUMERIC '0';

SET eligible_row_count = (
  SELECT COUNT(*)
  FROM eligible_rows
);

ASSERT
  eligible_row_count > 0
AS 'no eligible Silver rows exist for the requested batch';

ASSERT (
  SELECT COUNT(*) - COUNT(DISTINCT event_id)
  FROM eligible_rows
) = 0
AS 'eligible Silver rows contain duplicate event IDs';

CREATE TEMP TABLE staging AS
SELECT
  symbol,
  DATE(candle_start_time) AS candle_date,
  candle_start_time,
  TIMESTAMP_ADD(
    candle_start_time,
    INTERVAL 1 MINUTE
  ) AS candle_end_time,
  ARRAY_AGG(
    price
    ORDER BY event_time, trade_id, event_id
    LIMIT 1
  )[OFFSET(0)] AS open_price,
  MAX(price) AS high_price,
  MIN(price) AS low_price,
  ARRAY_AGG(
    price
    ORDER BY event_time DESC, trade_id DESC, event_id DESC
    LIMIT 1
  )[OFFSET(0)] AS close_price,
  SUM(quantity) AS total_volume,
  SUM(trade_value) AS quote_volume,
  COUNT(*) AS trade_count,
  SUM(
    IF(trade_side = 'BUY', quantity, NUMERIC '0')
  ) AS buy_volume_taker,
  SUM(
    IF(trade_side = 'SELL', quantity, NUMERIC '0')
  ) AS sell_volume_maker,
  COUNTIF(
    trade_value > NUMERIC '10000'
  ) AS large_trade_count,
  @source_batch AS source_batch,
  MIN(event_time) AS source_min_event_time,
  MAX(event_time) AS source_max_event_time
FROM eligible_rows
GROUP BY
  symbol,
  candle_start_time;

ASSERT (
  SELECT COUNT(*)
  FROM staging
) > 0
AS 'staging produced no one-minute candles';

ASSERT (
  SELECT COUNT(*)
  FROM (
    SELECT
      symbol,
      candle_start_time
    FROM staging
    GROUP BY
      symbol,
      candle_start_time
    HAVING COUNT(*) > 1
  )
) = 0
AS 'staging contains duplicate business keys';

ASSERT (
  SELECT COUNTIF(
    open_price <= NUMERIC '0'
    OR high_price <= NUMERIC '0'
    OR low_price <= NUMERIC '0'
    OR close_price <= NUMERIC '0'
  )
  FROM staging
) = 0
AS 'staging contains a non-positive OHLC value';

ASSERT (
  SELECT COUNTIF(
    low_price > open_price
    OR low_price > close_price
    OR high_price < open_price
    OR high_price < close_price
    OR high_price < low_price
  )
  FROM staging
) = 0
AS 'staging contains an invalid OHLC relationship';

ASSERT (
  SELECT COUNTIF(
    total_volume < NUMERIC '0'
    OR quote_volume < NUMERIC '0'
    OR trade_count < 0
    OR buy_volume_taker < NUMERIC '0'
    OR sell_volume_maker < NUMERIC '0'
    OR large_trade_count < 0
    OR large_trade_count > trade_count
  )
  FROM staging
) = 0
AS 'staging contains an invalid non-negative measure';

ASSERT (
  SELECT COUNTIF(
    buy_volume_taker + sell_volume_maker != total_volume
  )
  FROM staging
) = 0
AS 'buy and sell volume do not reconcile to total volume';

ASSERT (
  SELECT COUNTIF(
    source_min_event_time < candle_start_time
    OR source_max_event_time >= candle_end_time
  )
  FROM staging
) = 0
AS 'source event timestamps cross a candle boundary';

ASSERT (
  SELECT COUNTIF(
    candle_date != @event_date
    OR candle_date != DATE(candle_start_time)
    OR source_batch != @source_batch
  )
  FROM staging
) = 0
AS 'staging lineage or candle date does not match the requested batch';

ASSERT (
  SELECT SUM(trade_count)
  FROM staging
) = eligible_row_count
AS 'staging trade counts do not reconcile to eligible Silver rows';

ASSERT (
  SELECT STRING_AGG(symbol, ',' ORDER BY symbol)
  FROM (
    SELECT DISTINCT symbol
    FROM staging
  )
) = expected_symbols_csv
AS 'staging symbol set does not match the expected production set';

-- Gold v1 does not support overlapping batches for the same candle key.
ASSERT (
  SELECT COUNT(*)
  FROM `binance-lakehouse-dev-1611.binance_gold_dev.fact_market_candles_1m` AS target
  INNER JOIN staging
    ON target.symbol = staging.symbol
    AND target.candle_start_time = staging.candle_start_time
  WHERE target.candle_date = @event_date
    AND target.source_batch != @source_batch
) = 0
AS 'a different source batch already owns a requested candle key';

IF @allow_mutation THEN
  MERGE `binance-lakehouse-dev-1611.binance_gold_dev.fact_market_candles_1m` AS target
  USING staging
  ON target.symbol = staging.symbol
    AND target.candle_start_time = staging.candle_start_time
    AND target.candle_date = @event_date
  WHEN MATCHED THEN
    UPDATE SET
      candle_date = staging.candle_date,
      candle_end_time = staging.candle_end_time,
      open_price = staging.open_price,
      high_price = staging.high_price,
      low_price = staging.low_price,
      close_price = staging.close_price,
      total_volume = staging.total_volume,
      quote_volume = staging.quote_volume,
      trade_count = staging.trade_count,
      buy_volume_taker = staging.buy_volume_taker,
      sell_volume_maker = staging.sell_volume_maker,
      large_trade_count = staging.large_trade_count,
      source_batch = staging.source_batch,
      source_min_event_time = staging.source_min_event_time,
      source_max_event_time = staging.source_max_event_time,
      gold_processed_at = run_timestamp,
      loaded_at = run_timestamp
  WHEN NOT MATCHED THEN
    INSERT (
      symbol,
      candle_date,
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
      source_batch,
      source_min_event_time,
      source_max_event_time,
      gold_processed_at,
      loaded_at
    )
    VALUES (
      staging.symbol,
      staging.candle_date,
      staging.candle_start_time,
      staging.candle_end_time,
      staging.open_price,
      staging.high_price,
      staging.low_price,
      staging.close_price,
      staging.total_volume,
      staging.quote_volume,
      staging.trade_count,
      staging.buy_volume_taker,
      staging.sell_volume_maker,
      staging.large_trade_count,
      staging.source_batch,
      staging.source_min_event_time,
      staging.source_max_event_time,
      run_timestamp,
      run_timestamp
    );
END IF;

SELECT
  @source_batch AS source_batch,
  @event_date AS event_date,
  @allow_mutation AS mutation_enabled,
  source_row_count AS source_rows,
  eligible_row_count AS eligible_rows,
  (SELECT COUNT(*) FROM staging) AS staging_rows,
  (SELECT COUNT(DISTINCT symbol) FROM staging) AS distinct_symbols,
  (SELECT SUM(trade_count) FROM staging) AS staged_trade_count,
  (SELECT SUM(large_trade_count) FROM staging) AS staged_large_trade_count,
  (
    SELECT COUNT(*)
    FROM `binance-lakehouse-dev-1611.binance_gold_dev.fact_market_candles_1m`
    WHERE candle_date = @event_date
      AND source_batch = @source_batch
  ) AS target_rows_after,
  (
    SELECT COALESCE(SUM(trade_count), 0)
    FROM `binance-lakehouse-dev-1611.binance_gold_dev.fact_market_candles_1m`
    WHERE candle_date = @event_date
      AND source_batch = @source_batch
  ) AS target_trade_count_after,
  run_timestamp AS evaluated_at;
