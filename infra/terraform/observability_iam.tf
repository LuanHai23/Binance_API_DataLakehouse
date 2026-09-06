resource "google_project_iam_member" "terraform_monitoring_editor" {
  project = var.project_id
  role    = "roles/monitoring.editor"
  member  = "serviceAccount:${var.terraform_service_account}"
}

resource "google_project_iam_member" "terraform_logging_config_writer" {
  project = var.project_id
  role    = "roles/logging.configWriter"
  member  = "serviceAccount:${var.terraform_service_account}"
}