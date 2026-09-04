resource "google_workflows_workflow" "silver_batch" {
  project = var.project_id
  name    = "binance-silver-${var.environment}"
  region  = var.region

  description     = "Orchestrates idempotent Bronze-to-Silver Spark batches"
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
    SILVER_BASE_URI       = "gs://${google_storage_bucket.silver.name}"
    SILVER_SCRIPT_URI     = "gs://${google_storage_bucket.spark_code.name}/jobs/silver/fd62bed67334/spark_batch_silver_transform.py"
    SILVER_CODE_VERSION   = "fd62bed"
  }

  source_contents = file(
    "${path.module}/workflows/silver_batch.yaml"
  )

  depends_on = [
    google_project_service.workload["workflows.googleapis.com"],
    google_project_iam_member.workflow_spark_orchestrator,
    google_service_account_iam_member.workflow_can_act_as_spark,
  ]
}