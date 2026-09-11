# Cloud Gold Acceptance Evidence

Status: **Accepted and operating automatically**

## Accepted scope

The managed GCP pipeline now runs automatically from Bronze through Silver to
native BigQuery Gold one-minute market candles.

- Target: `binance_gold_dev.fact_market_candles_1m`
- Business key: `(symbol, candle_start_time)`
- Partition key: `candle_date`
- Clustering key: `symbol`
- Workflow revision: `000004-352`
- Automatic acceptance date: `2026-09-11`

## First unattended Gold cycle

| Evidence | Accepted value |
|---|---:|
| Workflow execution | `ff1b6788-e562-4698-8347-5757f3f76585` |
| Silver batch | `silver-wf-20260911t03z-99dbe12` |
| Source batch | `20260911t03z` |
| Gold query job | `job_nfB4YZ4wc7R-cabqUeTuhb4XXG0g` |
| Source rows | 64,270 |
| Unique source event IDs | 64,270 |
| Gold candle rows | 440 |
| Gold trade count | 64,270 |
| Large-trade count | 1,732 |
| Symbols | 8 |
| Duplicate business keys | 0 |
| Expected minus persisted | 0 |
| Persisted minus expected | 0 |

The unattended Workflow started at `2026-09-11T04:10:02Z` and completed at
`2026-09-11T04:13:41Z`.

## Quality acceptance

Independent reconstruction from Silver confirmed:

- Source and eligible row counts reconcile exactly.
- Persisted Gold candles equal independently reconstructed candles.
- OHLC relationships are valid.
- Prices, volumes, trade counts, and large-trade counts are valid.
- Candle dates and one-minute boundaries are valid.
- Source-event boundaries remain within the accepted UTC hour.
- Business keys are unique.
- Processing and loading audit fields are valid.
- No unrelated batch rows exist in the accepted date partition.

## Idempotency evidence

The initial controlled batch `20260909t03z` produced 445 candles from 50,917
Silver trades. A controlled same-batch rerun retained exactly 445 business keys,
created no duplicate rows, and reproduced the same deterministic values.

## Security and operational controls

- Cloud Scheduler triggers the pipeline once daily.
- Workflows orchestrates Silver and Gold under a dedicated service account.
- The Workflow service account can create BigQuery jobs.
- Silver read and Gold edit access are granted at table scope.
- Dataset and table deletion protection remain enabled.
- Queries enforce a maximum-bytes-billed guardrail.
- Cross-principal job inspection is not granted to the Terraform service
  account; the resulting HTTP 403 is an intentional least-privilege boundary.

## Acceptance declaration

The daily Bronze-to-Silver-to-Gold path is accepted as automated, deterministic,
idempotent, reconciled, protected, and suitable for the development portfolio
environment.

BI-facing marts, dashboards, controlled historical backfill, and advanced CI/CD
remain subsequent delivery phases.
