resource "google_cloud_scheduler_job" "binance_ingestor_hourly" {
  project     = var.project_id
  region      = var.region
  name        = "binance-ingestor-hourly-${var.environment}"
  description = "Start the bounded Binance ingestion Cloud Run job each hour"

  schedule  = "0 * * * *"
  time_zone = "Etc/UTC"
  paused    = true

  attempt_deadline = "60s"

  http_target {
    uri         = "https://run.googleapis.com/v2/${google_cloud_run_v2_job.binance_ingestor.id}:run"
    http_method = "POST"

    headers = {
      "Content-Type" = "application/json"
    }

    body = base64encode("{}")

    oauth_token {
      service_account_email = google_service_account.binance_scheduler.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [
    google_project_service.workload["cloudscheduler.googleapis.com"],
    google_project_iam_member.scheduler_pipeline_invoker,
    google_service_account_iam_member.terraform_can_act_as_scheduler,
  ]
}

resource "google_cloud_scheduler_job" "silver_hourly" {
  project     = var.project_id
  region      = var.region
  name        = "binance-silver-hourly-${var.environment}"
  description = "Process the previous completed UTC hour into Silver"

  schedule  = "10 * * * *"
  time_zone = "Etc/UTC"
  paused    = true

  attempt_deadline = "60s"

  retry_config {
    retry_count          = 1
    max_retry_duration   = "300s"
    min_backoff_duration = "30s"
    max_backoff_duration = "60s"
    max_doublings        = 1
  }

  http_target {
    uri         = "https://workflowexecutions.googleapis.com/v1/${google_workflows_workflow.silver_batch.id}/executions"
    http_method = "POST"

    headers = {
      "Content-Type" = "application/json"
    }

    body = base64encode(jsonencode({
      argument = "{}"
    }))

    oauth_token {
      service_account_email = google_service_account.binance_scheduler.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [
    google_project_service.workload["cloudscheduler.googleapis.com"],
    google_project_service.workload["workflowexecutions.googleapis.com"],
    google_project_iam_member.scheduler_pipeline_invoker,
    google_service_account_iam_member.terraform_can_act_as_scheduler,
  ]
}