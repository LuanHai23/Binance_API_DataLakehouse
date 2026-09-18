resource "google_project_iam_custom_role" "pipeline_scheduler" {
  project     = var.project_id
  role_id     = "binancePipelineScheduler"
  title       = "Binance Pipeline Scheduler"
  description = "Minimal permissions for Cloud Scheduler to start ingestion and Silver orchestration."
  stage       = "GA"

  permissions = [
    "run.jobs.run",
    "workflows.executions.create",
  ]

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_service_account" "binance_scheduler" {
  project      = var.project_id
  account_id   = "binance-scheduler-${var.environment}"
  display_name = "Binance Scheduler - ${var.environment}"
  description  = "Authenticated Cloud Scheduler identity for the Binance ingestion pipeline"

  deletion_policy = "PREVENT"
}

resource "google_project_iam_member" "scheduler_pipeline_invoker" {
  project = var.project_id
  role    = google_project_iam_custom_role.pipeline_scheduler.name
  member  = google_service_account.binance_scheduler.member
}

resource "google_service_account_iam_member" "terraform_can_act_as_scheduler" {
  service_account_id = google_service_account.binance_scheduler.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}