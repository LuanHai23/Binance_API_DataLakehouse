# Grant the Silver Workflow only the BigQuery capabilities required to run the
# guarded Gold MERGE: create its own query job, read the exact Silver source
# table, and edit the exact Gold target table.

resource "google_project_iam_custom_role" "workflow_gold_job_runner" {
  project     = var.project_id
  role_id     = "binanceGoldJobRunner"
  title       = "Binance Gold Job Runner"
  description = "Lets the Silver Workflow submit its guarded BigQuery Gold job."
  stage       = "GA"

  permissions = [
    "bigquery.jobs.create",
  ]

  deletion_policy = "PREVENT"

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_project_iam_member" "workflow_gold_job_runner" {
  project = var.project_id
  role    = google_project_iam_custom_role.workflow_gold_job_runner.name
  member  = "serviceAccount:${google_service_account.binance_workflow.email}"
}

resource "google_bigquery_table_iam_member" "workflow_silver_aggtrade_reader" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver.dataset_id
  table_id   = google_bigquery_table.silver_aggtrade.table_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.binance_workflow.email}"
}

resource "google_bigquery_table_iam_member" "workflow_gold_candle_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id   = google_bigquery_table.gold_fact_market_candles_1m.table_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.binance_workflow.email}"
}
