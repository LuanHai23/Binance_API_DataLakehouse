# Cloud Gold v1 Data Contract

Status: **Design baseline for implementation**

This contract defines the first native BigQuery Gold model built from the
accepted Silver BigLake table. It preserves the useful semantics of the local
Gold pipeline while making aggregation deterministic, idempotent, and safe for
the current once-daily cloud operating profile.

## Scope

- Source: `binance_silver_dev.aggtrade`
- Target dataset: `binance_gold_dev`
- Target table: `fact_market_candles_1m`
- Grain: one row per `symbol` and one-minute UTC candle
- Business key: `(symbol, candle_start_time)`
- Initial execution mode: one validated `source_batch` at a time
- Initial accepted batch: `20260909t03z`
- Gold orchestration: extend the existing Silver Workflow after Silver succeeds
- Out of scope: BI dashboards, historical backfill, cross-batch late-arrival
  correction, and advanced CI/CD promotion

`source_batch` is lineage metadata. It is not part of the business key.

## Source contract

The source is the Terraform-managed BigLake external table:

```text
binance-lakehouse-dev-1611.binance_silver_dev.aggtrade
```

Every production query must filter the Hive partition columns because the
source table requires a partition filter:

- `source_batch`
- `event_date`
- `symbol` when a narrower scan is appropriate

Cloud Silver provides these payload columns used by Gold:

| Source column | Type | Gold use |
|---|---|---|
| `event_id` | STRING | Deterministic tie-breaker and reconciliation |
| `trade_id` | INT64 | Deterministic trade ordering |
| `price` | NUMERIC | OHLC measures |
| `quantity` | NUMERIC | Base-asset volume |
| `trade_value` | NUMERIC | Quote-asset volume and large-trade rule |
| `event_time` | TIMESTAMP | Candle assignment and ordering |
| `trade_side` | STRING | Buy and sell volume |
| `source_batch` | STRING | Batch lineage and bounded processing |
| `event_date` | DATE | Required source partition filter |
| `symbol` | STRING | Candle dimension and clustering |

Rows are eligible for Gold only when `symbol`, `event_id`, `event_time`,
`price`, `quantity`, and `trade_value` are non-null; `price` and `quantity`
must also be positive.

## Target schema

| Column | BigQuery type | Mode | Definition |
|---|---|---|---|
| `symbol` | STRING | REQUIRED | Binance trading pair |
| `candle_date` | DATE | REQUIRED | UTC date of `candle_start_time` |
| `candle_start_time` | TIMESTAMP | REQUIRED | Inclusive UTC minute boundary |
| `candle_end_time` | TIMESTAMP | REQUIRED | Exclusive boundary, start plus one minute |
| `open_price` | NUMERIC | REQUIRED | First ordered price in the minute |
| `high_price` | NUMERIC | REQUIRED | Maximum price in the minute |
| `low_price` | NUMERIC | REQUIRED | Minimum price in the minute |
| `close_price` | NUMERIC | REQUIRED | Last ordered price in the minute |
| `total_volume` | NUMERIC | REQUIRED | Sum of base-asset quantity |
| `quote_volume` | NUMERIC | REQUIRED | Sum of `trade_value` |
| `trade_count` | INT64 | REQUIRED | Number of accepted Silver trades |
| `buy_volume_taker` | NUMERIC | REQUIRED | Quantity where `trade_side = 'BUY'` |
| `sell_volume_maker` | NUMERIC | REQUIRED | Quantity where `trade_side = 'SELL'` |
| `large_trade_count` | INT64 | REQUIRED | Trades whose quote value is above 10,000 USDT |
| `source_batch` | STRING | REQUIRED | Batch that most recently produced the candle |
| `source_min_event_time` | TIMESTAMP | REQUIRED | Earliest event timestamp represented |
| `source_max_event_time` | TIMESTAMP | REQUIRED | Latest event timestamp represented |
| `gold_processed_at` | TIMESTAMP | REQUIRED | Time the Gold aggregation ran |
| `loaded_at` | TIMESTAMP | REQUIRED | Time the target row was inserted or updated |

All financial measures remain `NUMERIC`; Cloud Gold must not downgrade them to
binary floating point.

## Physical design

- Native BigQuery table, not an external table
- Daily time partitioning on `candle_date`
- Clustering by `symbol`
- `require_partition_filter = true`
- Deletion protection enabled
- Terraform-managed dataset and table
- Dataset location matches Silver: `asia-southeast1`

Consumers must filter `candle_date`. Operational jobs should additionally
filter `source_batch` when reconciling one run.

## Deterministic candle semantics

For each `(symbol, TIMESTAMP_TRUNC(event_time, MINUTE))` group:

- `candle_start_time = TIMESTAMP_TRUNC(event_time, MINUTE)`
- `candle_end_time = TIMESTAMP_ADD(candle_start_time, INTERVAL 1 MINUTE)`
- `open_price` is selected by ascending `(event_time, trade_id, event_id)`
- `close_price` is selected by descending `(event_time, trade_id, event_id)`
- `high_price = MAX(price)`
- `low_price = MIN(price)`
- `total_volume = SUM(quantity)`
- `quote_volume = SUM(trade_value)`
- `trade_count = COUNT(*)`
- `buy_volume_taker = SUM(IF(trade_side = 'BUY', quantity, 0))`
- `sell_volume_maker = SUM(IF(trade_side = 'SELL', quantity, 0))`
- `large_trade_count = COUNTIF(trade_value > NUMERIC '10000')`

The local Spark prototype used unordered `first` and `last` aggregations.
Cloud Gold must use ordered array aggregation or an equivalent deterministic
expression so reruns cannot change open or close prices after a shuffle.

The large-trade threshold is quote currency. Gold v1 is valid because every
configured source symbol is quoted in USDT. Multi-quote support requires an
explicit normalized threshold policy before adding other quote assets.

## Idempotent load contract

Each batch is built into a staging relation and validated before mutation. A
single BigQuery `MERGE` then writes the target using the business key:

```sql
ON target.symbol = staging.symbol
AND target.candle_start_time = staging.candle_start_time
```

- `WHEN MATCHED` updates all measures, lineage, and processing timestamps.
- `WHEN NOT MATCHED` inserts the complete candle.
- Staging must contain exactly one row per business key.
- The production target is not mutated if any pre-merge validation fails.
- Reprocessing the same unchanged batch must not increase business-key count.

No delete clause is allowed in Gold v1.

## Pre-merge quality gates

The Gold job must stop before `MERGE` unless all checks pass:

1. The requested `source_batch` matches the expected batch-id pattern.
2. At least one eligible Silver row exists for the batch.
3. No staging business key is duplicated.
4. Every OHLC value is positive.
5. `low_price <= open_price`, `low_price <= close_price`,
   `high_price >= open_price`, and `high_price >= close_price`.
6. `high_price >= low_price`.
7. Volumes and counts are non-negative.
8. `buy_volume_taker + sell_volume_maker = total_volume`.
9. `source_min_event_time >= candle_start_time`.
10. `source_max_event_time < candle_end_time`.
11. `SUM(staging.trade_count)` equals the eligible Silver input-row count.
12. Staging contains the expected symbol set for the validated batch.

NUMERIC comparisons should be exact. Do not introduce a floating-point
tolerance for volume reconciliation.

## Post-merge acceptance gates

For the processed batch and candle date:

- Target business keys are unique.
- Target row count equals staging row count.
- Target `SUM(trade_count)` equals eligible Silver row count.
- Target `SUM(large_trade_count)` is between zero and `SUM(trade_count)`.
- Minimum and maximum source event times remain within candle boundaries.
- A second execution of the same batch preserves row count and business keys.
- Terraform reports no infrastructure changes after deployment.

The first acceptance run uses Silver batch `20260909t03z`, whose accepted
baseline contains 50,917 rows, eight symbols, zero rejected rows, zero duplicate
event IDs, and zero quarantine rows. The number of one-minute candles is a
measured acceptance result, not a hard-coded expectation.

## Orchestration contract

Gold runs only after the existing Workflow confirms that the Silver Dataproc
batch succeeded. The Workflow passes the exact `source_batch` and event date to
the Gold job.

There is no independent clock-based Gold Scheduler in v1. This prevents Gold
from racing an incomplete Silver batch and preserves a single lineage chain:

```text
Scheduler -> Silver Workflow -> Dataproc Silver -> Gold validation -> Gold MERGE
```

On Gold failure, the Workflow fails, Monitoring alerts, and the next automatic
cycle must not silently mark the failed Gold batch as accepted.

## Implementation sequence

1. Add Terraform for the native Gold dataset and protected candle table.
2. Apply and verify only the reviewed infrastructure additions.
3. Implement parameterized deterministic aggregation and atomic `MERGE`.
4. Run a read-only staging query for `20260909t03z` and review all gates.
5. Execute the first controlled Gold merge and reconcile it to 50,917 trades.
6. Rerun the same batch to prove idempotency.
7. Extend the existing Workflow and failure alert path.
8. Observe the first unattended Silver-to-Gold cycle.
9. Document acceptance evidence before starting BI models.

Any controlled historical backfill is a separate capability and must define
its own range bounds, collision behavior, cost estimate, and acceptance plan.
