# Silver and Gold run history

This change adds automatic stage history to `binance_ops_dev.pipeline_run_audit`.
It is based on commit `a9aaab7`. The existing Spark artifact, Gold SQL, batch
identity, polling limits and success response remain unchanged.

## How it works

1. Before submitting Silver, the Workflow writes a `RUNNING` row.
2. After observing a successful Dataproc batch, it updates Silver to `SUCCEEDED`
   and creates a `RUNNING` row for Gold.
3. After the existing Gold reconciliation passes, it updates Gold to `SUCCEEDED`.
4. A caught pipeline error updates the active stage to `FAILED` and is re-raised.
   The Workflow therefore retains its original failure and existing alerts.

`run_id` is the Workflow execution ID. A rerun of the same `source_batch` has a
different run ID and keeps its own history. The logical key is
`(run_date, run_id, stage)`. `run_date` is the UTC date when the Workflow started,
not the source data date; it stays the same if Gold starts after midnight.

The SQL uses a parameterized multi-statement transaction: a partition-pruned
`UPDATE` followed by a conditional `INSERT`. Every target read has the direct
`run_date = @run_date` predicate required by the partition filter. An event
timestamp prevents an older write from replacing a newer result. BigQuery does
not enforce uniqueness on this key; this is a single-Workflow-writer design,
not a general exactly-once guarantee for concurrent writers.

`resource_name` references the Dataproc batch or BigQuery Gold job when known.
Gold counts come from its existing result: Silver source rows in `input_rows`
and persisted candle rows in `output_rows`. Silver counts remain `NULL` because
the batch status API does not provide them. Unknown is not zero.

## Where the code lives

| File | Responsibility |
| --- | --- |
| `infra/terraform/workflows/silver_batch.yaml` | Stage transitions and original-error handling |
| `infra/terraform/workflows/run_audit.yaml` | Small shared helper that submits the audit query |
| `sql/gcp_ops/upsert_pipeline_run.sql.tftpl` | Parameterized insert/update of run history |
| `infra/terraform/silver_workflow.tf` | Assemble Workflow source with both SQL statements |
| `infra/terraform/bigquery_observability.tf` | Grant the Workflow access to the audit table and logs |

The BigQuery `jobs.insert` connector waits for its job to complete, so the audit
helper does not need another hand-written polling loop. The helper bounds query
bytes and waiting time. See the [connector reference](https://docs.cloud.google.com/workflows/docs/reference/googleapis/bigquery/v2/jobs/insert).

## Error handling and scope

Audit writes are best effort. If a write fails, the helper attempts an ERROR log
with event `PIPELINE_RUN_AUDIT_WRITE_FAILED`, run ID, stage and error details.
It does not replace the pipeline's error or turn an otherwise successful data
run into a failure. A separate log-writer permission enables these custom logs.
See [Workflow logging permissions](https://docs.cloud.google.com/workflows/docs/log-workflow).

A Workflow cancellation or forced termination can bypass the error handler and
leave a row `RUNNING`. A timeout recorded as `FAILED` describes the Workflow stage
observation; the referenced remote job might still be running. Follow the job ID
to confirm its state. Missing/stale-run detection is a separate follow-up.

If Silver fails, there is no Gold row because Gold was not started. A reused
Dataproc batch can produce a successful Silver stage observation without a new
Spark execution; stage timestamps are Workflow observation times.

This increment does not populate `dq_check_results` or audit Cloud Run ingestion.
It does not treat stage success as a substitute for per-check DQ evidence.

## Deployment and acceptance

The table permissions and logging permission were managed before the final
Workflow deployment. The final reviewed Terraform plan contained exactly one
Workflow source update: `0` resources added, `1` changed and `0` destroyed. No
table schema, Spark artifact or Gold SQL changed in that plan.

Local checks parsed HCL/YAML and SQL, checked that the original processing steps
were preserved, and exercised insert, update, replay, late-event ordering,
midnight dates, NULL counts and stage-specific failures using a SQL translation
to DuckDB. These checks do not replace Google Cloud compilation or runtime tests.

After deployment, query audit rows for the execution that was actually tested.
Use the execution start date and UUID as named parameters:

```sql
SELECT
  run_id, source_batch, stage, status, started_at, finished_at,
  input_rows, output_rows, resource_name, error_message
FROM `binance-lakehouse-dev-1611.binance_ops_dev.pipeline_run_audit`
WHERE run_date = @run_date
  AND run_id = @run_id
ORDER BY started_at, stage;
```

For a successful run, expect one Silver and one Gold `SUCCEEDED` row with the
same run ID and source batch. Gold counts must match the Workflow result.
For a caught failure, expect `FAILED` on the active stage and the original
Workflow error. Inspect Cloud Logging if an expected audit row is missing.

### Accepted runtime evidence

The controlled acceptance run on 2026-09-16 passed on Workflow revision
`000008-32a`:

| Property | Accepted value |
| --- | --- |
| Workflow execution | `4bb717f6-788f-4c52-a3a0-8bb5e4469e75` |
| Source batch | `20260916t03z` |
| Dataproc batch | `silver-wf-20260916t03z-e2ab277` |
| Dataproc submission | `ALREADY_EXISTS` |
| Gold job | `job_W5gJ--i1fL7bAa7EwtgpbL7PuAfg` |
| Gold input rows | `57,952` |
| Gold output candle rows | `447` |
| Audit rows | `2` (`silver`, `gold`) |
| Final stage states | both `SUCCEEDED` |

`ALREADY_EXISTS` proves that this acceptance reused the previously successful
Dataproc batch instead of starting another Spark batch. The guarded Gold job
reconciled to the accepted baseline, and a bounded partition-filtered query
confirmed the two final audit rows with no error message.

`PIPELINE_RUN_AUDIT_ACCEPTANCE=PASS`
