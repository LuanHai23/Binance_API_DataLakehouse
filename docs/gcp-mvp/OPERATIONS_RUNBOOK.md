# GCP MVP Operations Runbook

This runbook covers the deployed daily Bronze-to-Silver-to-BigLake development pipeline.

## Service map

| Stage | Resource | Normal state |
|---|---|---|
| Trigger | `binance-ingestor-hourly-dev` | ENABLED; daily at 03:00 UTC |
| Ingestion | `binance-ingestor-dev` | No active execution outside its window |
| Bronze | `<project>-bronze` | JSONL objects for the scheduled hour |
| Trigger | `binance-silver-hourly-dev` | ENABLED; daily at 04:10 UTC |
| Orchestration | `binance-silver-dev` | Latest scheduled execution SUCCEEDED |
| Transform | Dataproc Serverless batch | Latest scheduled batch SUCCEEDED |
| Silver | `<project>-silver` | Eight symbol partitions for a healthy batch |
| Catalog | `binance_silver_dev.aggtrade` | Queryable with a partition filter |

## Session setup

```bash
cd ~/Binance_API_DataLakehouse
source .venv/bin/activate
export PROJECT_ID="binance-lakehouse-dev-1611"
export REGION="asia-southeast1"
export TF_SA="tf-binance-dev@${PROJECT_ID}.iam.gserviceaccount.com"
export INGESTOR_SCHEDULER="binance-ingestor-hourly-dev"
export SILVER_SCHEDULER="binance-silver-hourly-dev"
gcloud config set project "${PROJECT_ID}"
```

Use service-account impersonation for infrastructure inspection. Use the active Owner identity only when a read operation is not granted to the Terraform service account.

## Expected daily timeline

| Vietnam time | Expected event |
|---:|---|
| 10:00 | Scheduler starts the Cloud Run Ingestor |
| 10:00-10:55 | Ingestor publishes aggTrade events |
| About 10:55 | Ingestor succeeds and Bronze delivery stabilizes |
| 11:10 | Scheduler starts the Silver Workflow |
| About 11:12 | Workflow and Dataproc batch complete |
| After 11:15 | Silver and BigLake acceptance checks are safe to run |

A few seconds of Scheduler jitter is normal. A missing attempt, failed execution, or incomplete data window is not.

## Daily health check

Check both Scheduler jobs first:

```bash
gcloud scheduler jobs describe "${INGESTOR_SCHEDULER}" --project="${PROJECT_ID}" --location="${REGION}" --impersonate-service-account="${TF_SA}" --format="yaml(name,state,schedule,timeZone,lastAttemptTime,scheduleTime)"
gcloud scheduler jobs describe "${SILVER_SCHEDULER}" --project="${PROJECT_ID}" --location="${REGION}" --impersonate-service-account="${TF_SA}" --format="yaml(name,state,schedule,timeZone,lastAttemptTime,scheduleTime)"
```

Healthy Scheduler criteria:

- Both states are `ENABLED`.
- Schedules remain `0 3 * * *` and `10 4 * * *` in `Etc/UTC`.
- `lastAttemptTime` matches the current daily cycle.
- `scheduleTime` points to the next daily cycle.

## Data acceptance criteria

A cycle is accepted only when all of the following hold:

- Cloud Run execution was created by the Scheduler service account.
- Ingestor completed with one success and zero failures.
- Bronze contains JSONL objects under the expected UTC-hour prefix.
- Workflow result is `SUCCEEDED` and references the expected input URI.
- Dataproc batch is `SUCCEEDED`.
- `input_rows = silver_rows = written_rows = unique_event_ids`.
- Rejected, duplicate, and quarantine row counts are zero.
- Eight expected symbol partitions exist.
- BigLake row count equals the Spark written-row count.
- Event IDs, timestamps, prices, quantities, and trade values pass validation.

### Enforced Silver pre-publish gates

The Spark transform writes and reads back the batch quarantine before deciding
whether Silver may be published. It stops before the Silver overwrite when:

- no valid row remains;
- any Bronze row is rejected by the schema or business validation rules;
- a duplicate `event_id` is detected before deduplication; or
- the accepted symbol set differs from the eight configured production pairs.

This development profile deliberately uses a strict, accuracy-first policy:
deduplication still creates the candidate Silver dataset, but any duplicate is
treated as an upstream anomaly rather than silently accepted. Rejected records
remain in the batch quarantine. Duplicate and symbol-set diagnostics remain in
the Dataproc driver logs, while the immutable Bronze input remains available
for investigation.

A failed pre-publish gate does not modify the intended Silver output. During a
rerun, an older same-batch Silver prefix may already exist, so its presence is
not proof of success; use the terminal Dataproc and Workflow states as the
authoritative result.

Cloud Silver v1 defines `trade_value` as the decimal product of `price` and
`quantity`, rounded to six fractional digits. This makes Spark's bounded
fixed-precision result explicit. Acceptance must compare the stored value
exactly with `ROUND(price * quantity, 6)`; it must not compare with the
unrounded product or introduce a floating-point tolerance.

Always filter the BigLake table by Hive partition keys. Example:

```sql
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT event_id) AS unique_event_ids,
  COUNT(DISTINCT symbol) AS distinct_symbols
FROM `binance-lakehouse-dev-1611.binance_silver_dev.aggtrade`
WHERE source_batch = 'YYYYMMDDtHHz'
  AND event_date = DATE 'YYYY-MM-DD';
```

Do not treat Scheduler delivery alone as pipeline success. Validate the terminal workload and the persisted data.

## Incident containment

If duplicate execution, uncontrolled cost, or repeated failure is suspected, pause both triggers:

```bash
gcloud scheduler jobs pause "${INGESTOR_SCHEDULER}" --project="${PROJECT_ID}" --location="${REGION}" --impersonate-service-account="${TF_SA}"
gcloud scheduler jobs pause "${SILVER_SCHEDULER}" --project="${PROJECT_ID}" --location="${REGION}" --impersonate-service-account="${TF_SA}"
```

Pausing a Scheduler prevents future triggers; it does not stop an execution already running.

Triage in this order:

1. Confirm the affected UTC batch and execution identity.
2. Inspect Cloud Monitoring email and incident metadata.
3. Inspect the Cloud Run execution or Workflow result.
4. Inspect Dataproc batch state and driver output when Silver failed.
5. Inventory Bronze and Silver objects for the exact batch.
6. Reconcile input, rejected, duplicate, written, and unique counts.
7. Record the root cause before deciding whether to rerun.

Do not manually rerun the Ingestor into an existing batch window without checking duplicate and overwrite behavior. Controlled backfill is a separate, not-yet-implemented capability.

## Recovery and re-enablement

Before re-enabling automation:

- The incident cause is understood or safely bounded.
- Terraform validates and reports no unintended changes.
- No unexpected Cloud Run or Workflow execution is active.
- The next Ingestor-to-Silver gap remains at least 70 minutes.
- Alerting and the monthly billing guardrail remain enabled.

Resume only the intended jobs and verify their next scheduled times immediately. Any permanent state change must also be represented in Terraform and committed to Git.

## Cost controls

- The pipeline runs once daily in development.
- The billing budget is an alert-only guardrail; it is not a hard spending cap.
- The configured monthly amount is VND 500,000.
- Review Billing reports after workload or schedule changes.
- Keep both Scheduler jobs paused during risky maintenance.

## Change procedure

1. Confirm the Git worktree is clean.
2. Pause triggers when the change can affect execution behavior.
3. Run `terraform validate` and review an exact saved plan.
4. Reject any unexpected create, update, replace, or destroy action.
5. Apply only the reviewed plan.
6. Verify live state and Terraform convergence.
7. Commit and push the matching source change.
8. Observe the first automatic cycle after material changes.

## Known phase boundaries

- Controlled historical backfill is not implemented.
- Cloud Gold models and BI dashboards are not part of this MVP.
- Advanced delivery promotion and rollback automation remain future CI/CD work.

For the immutable acceptance baseline, see [Acceptance evidence](ACCEPTANCE_EVIDENCE.md).

### Silver DQ runtime acceptance — 2026-09-13 UTC

Batch `20260912t03z`, Spark artifact `e2ab277`, Workflow revision `000006-fe1`.
Execution `b82db9c3-d4ef-4a6d-815f-ef4f914dc0ad` and Dataproc both succeeded.
Post-run SELECT: 38146 Silver rows and unique event IDs; 8 symbols; 0 invalid rows.
Gold: 438 rows and unique keys; 38146 trades; 744 large trades; 8 symbols.
Silver processed at `2026-09-13 11:33:09.843968+00`;
Gold processed and loaded at `2026-09-13 11:33:36.388558+00`.
