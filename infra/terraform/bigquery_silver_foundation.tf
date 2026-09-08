resource "google_bigquery_dataset" "silver" {
  project    = var.project_id
  dataset_id = "binance_silver_${var.environment}"

  friendly_name = "Binance Silver ${var.environment}"
  description   = "Curated Binance aggTrade data catalogued from Silver Parquet."
  location      = var.region

  delete_contents_on_destroy = false

  labels = {
    component  = "silver"
    data_layer = "silver"
  }

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    google_project_iam_member.terraform_bigquery["roles/bigquery.user"],
  ]
}

resource "google_bigquery_connection" "silver_biglake" {
  project       = var.project_id
  location      = var.region
  connection_id = "binance-silver-${var.environment}"

  friendly_name = "Binance Silver ${var.environment} BigLake"
  description   = "Delegated Cloud Storage access for the Silver BigLake catalog."

  cloud_resource {}

  depends_on = [
    google_project_iam_member.terraform_bigquery["roles/bigquery.connectionAdmin"],
    google_project_service.workload["bigqueryconnection.googleapis.com"],
  ]
}

resource "google_storage_bucket_iam_member" "biglake_silver_reader" {
  bucket = google_storage_bucket.silver.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_bigquery_connection.silver_biglake.cloud_resource[0].service_account_id}"
}
