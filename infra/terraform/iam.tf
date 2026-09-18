resource "google_service_account" "binance_ingestor" {
  project         = var.project_id
  account_id      = "binance-ingestor-dev"
  display_name    = "Binance Ingestor - dev"
  description     = "Runtime identity for the Binance ingestion workload"
  deletion_policy = "PREVENT"
}

resource "google_service_account_iam_member" "terraform_can_act_as_ingestor" {
  service_account_id = google_service_account.binance_ingestor.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}