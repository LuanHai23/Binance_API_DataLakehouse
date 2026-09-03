resource "google_project_iam_custom_role" "spark_orchestrator" {
  project     = var.project_id
  role_id     = "binanceSparkOrchestrator"
  title       = "Binance Spark Orchestrator"
  description = "Submit and observe Managed Service for Apache Spark batches without cancel or delete permissions."
  stage       = "GA"

  permissions = [
    "dataproc.batches.create",
    "dataproc.batches.get",
    "dataproc.operations.get",
  ]

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_service_account" "binance_workflow" {
  project      = var.project_id
  account_id   = "binance-workflow-${var.environment}"
  display_name = "Binance Workflow - ${var.environment}"
  description  = "Runtime identity for Binance lakehouse orchestration workflows"

  deletion_policy = "PREVENT"
}

resource "google_project_iam_member" "workflow_spark_orchestrator" {
  project = var.project_id
  role    = google_project_iam_custom_role.spark_orchestrator.name
  member  = google_service_account.binance_workflow.member
}

resource "google_service_account_iam_member" "workflow_can_act_as_spark" {
  service_account_id = google_service_account.binance_spark.name
  role               = "roles/iam.serviceAccountUser"
  member             = google_service_account.binance_workflow.member
}

resource "google_service_account_iam_member" "terraform_can_act_as_workflow" {
  service_account_id = google_service_account.binance_workflow.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}