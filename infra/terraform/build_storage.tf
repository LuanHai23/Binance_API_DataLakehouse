resource "google_storage_bucket" "cloud_build_source" {
  project  = var.project_id
  name     = "${var.project_id}-cloud-build-source"
  location = upper(var.region)

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle_rule {
    condition {
      age        = 1
      with_state = "ANY"
    }

    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket_iam_member" "builder_cloud_build_source_reader" {
  bucket = google_storage_bucket.cloud_build_source.name
  role   = "roles/storage.objectViewer"
  member = google_service_account.binance_builder.member
}