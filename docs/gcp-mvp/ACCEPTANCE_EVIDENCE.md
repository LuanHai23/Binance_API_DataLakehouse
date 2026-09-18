# GCP MVP Acceptance Evidence

## Decision

**Result: PASS**

The deployed Bronze-to-Silver-to-BigLake MVP was accepted after a complete unattended daily cycle on 2026-09-09. No manual trigger was used for the accepted cycle.

## Acceptance scope

| Property | Value |
|---|---|
| Project | `binance-lakehouse-dev-1611` |
| Region | `asia-southeast1` |
| Environment | `dev` |
| Source batch | `20260909t03z` |
| Event date | `2026-09-09` |
| Ingestor schedule | `0 3 * * *` in `Etc/UTC` |
| Silver schedule | `10 4 * * *` in `Etc/UTC` |

The accepted path was Cloud Scheduler to Cloud Run to Pub/Sub to Bronze, followed by Cloud Scheduler to Workflows to Dataproc Serverless to Silver and BigLake.

## Execution identities

| Component | Accepted execution |
|---|---|
| Cloud Run Job | `binance-ingestor-dev-mdv8c` |
| Workflow | `8ff4b703-33a3-431a-978f-5a75767a0943` |
| Dataproc batch | `silver-wf-20260909t03z-99dbe12` |
| BigLake table | `binance_silver_dev.aggtrade` |

The Cloud Run execution creator was `binance-scheduler-dev@binance-lakehouse-dev-1611.iam.gserviceaccount.com`, confirming that the accepted ingestion was Scheduler-initiated.

## Ingestor and Bronze evidence

| Measurement | Result |
|---|---|
| Scheduled creation | `2026-09-09T03:00:01.004272Z` |
| Execution start | `2026-09-09T03:00:04.008810Z` |
| Execution completion | `2026-09-09T03:55:18.698604Z` |
| Runtime result | 1 succeeded, 0 failed |
| Reported duration | 55m14.68s |
| Bronze prefix | `gs://binance-lakehouse-dev-1611-bronze/raw/aggtrade/2026/09/09/03/` |
| Bronze JSONL objects | 388 |

**Ingestor-to-Bronze result: PASS**

## Workflow and Dataproc evidence

| Measurement | Result |
|---|---|
| Workflow start | `2026-09-09T04:10:00.965688040Z` |
| Workflow end | `2026-09-09T04:12:06.439075574Z` |
| Workflow state | `SUCCEEDED` |
| Workflow revision | `000003-088` |
| Window mode | `previous_hour` |
| Input URI | `gs://binance-lakehouse-dev-1611-bronze/raw/aggtrade/2026/09/09/03/*.jsonl` |
| Dataproc create time | `2026-09-09T04:10:01.695285Z` |
| Dataproc terminal state | `SUCCEEDED` |

The Workflow selected source batch `20260909t03z` and submitted the expected Dataproc batch without a manual invocation.

## Spark reconciliation

| Metric | Count |
|---|---:|
| Input rows | 50,917 |
| Rejected rows | 0 |
| Duplicate rows removed | 0 |
| Silver rows | 50,917 |
| Written Silver rows | 50,917 |
| Written unique event IDs | 50,917 |
| Written quarantine rows | 0 |
| Silver Parquet files | 8 |

**Workflow-to-Dataproc-to-Silver result: PASS**

## BigLake row-level evidence

The partition-pruned acceptance query returned:

| Check | Result |
|---|---:|
| Total rows | 50,917 |
| Unique event IDs | 50,917 |
| Distinct symbols | 8 |
| Invalid event IDs | 0 |
| Invalid event times | 0 |
| Invalid trade values | 0 |
| Minimum event time | `2026-09-09 03:00:16` |
| Maximum event time | `2026-09-09 03:55:15` |

The BigLake catalog has 23 fields and uses custom Hive partition keys `source_batch`, `event_date`, and `symbol`. Partition filtering is required.

**Silver-to-BigLake quality result: PASS**

## Recurring automation evidence

After acceptance, both Scheduler jobs remained `ENABLED`:

| Scheduler | Next scheduled execution | Observed jitter |
|---|---|---:|
| Ingestor | `2026-09-10T03:00:04.067257Z` | 4s |
| Silver | `2026-09-10T04:10:01.497909Z` | 1s |

The next daily pair remained scheduled with the intended 70-minute separation.

## Control-plane evidence

- Terraform validation passed.
- Terraform refresh and plan reported no changes after deployment.
- Ingestor and Silver failure policies were enabled with `ERROR` severity.
- Alert delivery to the configured email notification channel was tested.
- The BigLake connection service account has read-only Silver object access.
- The BigLake table has deletion protection and required partition filtering.
- The monthly billing guardrail is VND 500,000 and alert-only.
- Terraform records the budget deletion policy as `PREVENT`.
- Git branch `feat/gcp-terraform` was synchronized with its remote.
- Automatic activation was recorded in commit `96c750d`.

## Acceptance matrix

| Requirement | Result |
|---|---|
| Genuine unattended Scheduler trigger | PASS |
| Bounded Ingestor execution | PASS |
| Bronze object delivery | PASS |
| Previous-hour Workflow selection | PASS |
| Dataproc Serverless transformation | PASS |
| Exact row reconciliation | PASS |
| Zero rejected and quarantine rows | PASS |
| Eight symbol partitions | PASS |
| BigLake query and data quality | PASS |
| Alerts and billing guardrail | PASS |
| Terraform convergence | PASS |
| Next recurring cycle retained | PASS |

## Final declaration

`UNATTENDED_DAILY_CYCLE=PASS`

The GCP Bronze-to-Silver-to-BigLake MVP is complete and operational. Cloud Gold models, BI dashboards, controlled backfill, and advanced CI/CD are subsequent project phases rather than acceptance blockers for this milestone.
