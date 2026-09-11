resource "google_workflows_workflow" "silver_batch" {
  project = var.project_id
  name    = "binance-silver-${var.environment}"
  region  = var.region

  description     = "Orchestrates idempotent Bronze-to-Silver-to-Gold batches"
  service_account = google_service_account.binance_workflow.id

  call_log_level      = "LOG_ERRORS_ONLY"
  deletion_protection = true

  labels = {
    component = "silver"
  }

  user_env_vars = {
    BINANCE_PROJECT_ID    = var.project_id
    BINANCE_REGION        = var.region
    BINANCE_ENVIRONMENT   = var.environment
    SPARK_SERVICE_ACCOUNT = google_service_account.binance_spark.email
    SPARK_SUBNETWORK      = google_compute_subnetwork.binance_spark.id
    SPARK_STAGING_BUCKET  = google_storage_bucket.spark_staging.name
    BRONZE_BASE_URI       = "gs://${google_storage_bucket.bronze.name}"
    SILVER_BASE_URI       = "gs://${google_storage_bucket.silver.name}"
    SILVER_SCRIPT_URI     = "gs://${google_storage_bucket.spark_code.name}/jobs/silver/99dbe126b590/spark_batch_silver_transform.py"
    SILVER_CODE_VERSION   = "99dbe12"
  }

  source_contents = replace(
    file("${path.module}/workflows/silver_batch.yaml"),
    "__GOLD_MERGE_QUERY_JSON__",
    jsonencode(file("${path.module}/../../sql/gcp_gold/fact_market_candles_1m_merge.sql")),
  )

  depends_on = [
    google_project_service.workload["workflows.googleapis.com"],
    google_project_iam_member.workflow_spark_orchestrator,
    google_service_account_iam_member.workflow_can_act_as_spark,
    google_project_iam_member.workflow_gold_job_runner,
    google_bigquery_table_iam_member.workflow_silver_aggtrade_reader,
    google_bigquery_table_iam_member.workflow_gold_candle_editor,
  ]
}
