# Audit keys are application contracts, not BigQuery uniqueness constraints.
# run_id identifies an execution; rerunning a source_batch gets a new run_id.
# Writers must preserve run_date (UTC execution start date) when updating a row.
# Run key: (run_date, run_id, stage).
# DQ key:  (run_date, run_id, stage, check_name).
# Writer contract (STRING enums are not enforced by the BigQuery schema):
# Run status: RUNNING, SUCCEEDED, FAILED, CANCELLED, TIMED_OUT or UNKNOWN.
# DQ status: PASS, FAIL, ERROR or SKIPPED; unevaluated checks are never PASS.
# Unknown counts stay NULL. resource_name points to the job/batch for debugging.
# This file creates storage only; pipeline writers and their IAM follow next.
locals {
  observability_tables = {
    pipeline_run_audit = {
      description = "One row per execution and stage; status, counts and error reference."
      schema = [
        { name = "run_date", type = "DATE", mode = "REQUIRED" },
        { name = "run_id", type = "STRING", mode = "REQUIRED" },
        { name = "source_batch", type = "STRING", mode = "NULLABLE" },
        { name = "stage", type = "STRING", mode = "REQUIRED" },
        { name = "status", type = "STRING", mode = "REQUIRED" },
        { name = "started_at", type = "TIMESTAMP", mode = "REQUIRED" },
        { name = "finished_at", type = "TIMESTAMP", mode = "NULLABLE" },
        { name = "input_rows", type = "INTEGER", mode = "NULLABLE" },
        { name = "output_rows", type = "INTEGER", mode = "NULLABLE" },
        { name = "resource_name", type = "STRING", mode = "NULLABLE" },
        { name = "error_message", type = "STRING", mode = "NULLABLE" },
        { name = "updated_at", type = "TIMESTAMP", mode = "REQUIRED" },
      ]
    }
    dq_check_results = {
      description = "One result per execution, stage and DQ check; includes failed or unevaluated checks."
      schema = [
        { name = "run_date", type = "DATE", mode = "REQUIRED" },
        { name = "run_id", type = "STRING", mode = "REQUIRED" },
        { name = "source_batch", type = "STRING", mode = "NULLABLE" },
        { name = "stage", type = "STRING", mode = "REQUIRED" },
        { name = "check_name", type = "STRING", mode = "REQUIRED" },
        { name = "status", type = "STRING", mode = "REQUIRED" },
        { name = "observed_value", type = "STRING", mode = "NULLABLE" },
        { name = "expected_value", type = "STRING", mode = "NULLABLE" },
        { name = "checked_at", type = "TIMESTAMP", mode = "REQUIRED" },
        { name = "error_message", type = "STRING", mode = "NULLABLE" },
      ]
    }
  }
}

resource "google_bigquery_dataset" "ops" {
  project       = var.project_id
  dataset_id    = "binance_ops_${var.environment}"
  friendly_name = "Binance pipeline operations ${var.environment}"
  description   = "Pipeline run history and data quality results for operational dashboards."
  location      = var.region

  deletion_policy            = "PREVENT"
  delete_contents_on_destroy = false

  labels = {
    component  = "observability"
    data_layer = "ops"
  }

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    google_project_iam_member.terraform_bigquery["roles/bigquery.user"],
  ]
}

resource "google_bigquery_table" "ops" {
  for_each = local.observability_tables

  project       = var.project_id
  dataset_id    = google_bigquery_dataset.ops.dataset_id
  table_id      = each.key
  friendly_name = "${each.key} ${var.environment}"
  description   = each.value.description
  schema        = jsonencode(each.value.schema)

  deletion_policy     = "PREVENT"
  deletion_protection = true
  resource_tags       = {}

  time_partitioning {
    type  = "DAY"
    field = "run_date"
  }

  require_partition_filter = true
  clustering               = ["stage", "status", "source_batch"]

  labels = {
    component  = "observability"
    data_layer = "ops"
  }
}
