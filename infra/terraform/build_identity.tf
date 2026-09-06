resource "google_service_account" "binance_builder" {
  project      = var.project_id
  account_id   = "binance-builder-${var.environment}"
  display_name = "Binance Container Builder - ${var.environment}"
  description  = "Least-privilege Cloud Build identity for Binance producer images"

  deletion_policy = "PREVENT"
}

resource "google_artifact_registry_repository_iam_member" "builder_producer_writer" {
  project    = google_artifact_registry_repository.binance_producer.project
  location   = google_artifact_registry_repository.binance_producer.location
  repository = google_artifact_registry_repository.binance_producer.repository_id
  role       = "roles/artifactregistry.writer"
  member     = google_service_account.binance_builder.member
}

resource "google_project_iam_member" "builder_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = google_service_account.binance_builder.member
}

resource "google_project_iam_member" "terraform_cloud_build_editor" {
  project = var.project_id
  role    = "roles/cloudbuild.builds.editor"
  member  = "serviceAccount:${var.terraform_service_account}"
}

resource "google_service_account_iam_member" "terraform_can_act_as_builder" {
  service_account_id = google_service_account.binance_builder.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}