data "google_project" "binance_lakehouse" {
  project_id = var.project_id
}

locals {
  pubsub_service_agent = "service-${data.google_project.binance_lakehouse.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_storage_bucket" "bronze" {
  project       = var.project_id
  name          = "${var.project_id}-bronze"
  location      = var.region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  requester_pays              = false
  force_destroy               = false

  versioning {
    enabled = false
  }

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age        = 7
      with_state = "LIVE"
    }
  }
}

resource "google_storage_bucket_iam_member" "pubsub_bronze_object_creator" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${local.pubsub_service_agent}"
}

resource "google_storage_bucket_iam_member" "pubsub_bronze_bucket_reader" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${local.pubsub_service_agent}"
}

resource "google_pubsub_subscription" "binance_aggtrade_bronze_storage" {
  project = var.project_id
  name    = "binance-aggtrade-bronze-storage-dev"
  topic   = google_pubsub_topic.binance_aggtrade_events.id

  deletion_policy            = "DELETE"
  message_retention_duration = "86400s"
  retain_acked_messages      = false

  expiration_policy {
    ttl = ""
  }

  cloud_storage_config {
    bucket                   = google_storage_bucket.bronze.name
    filename_prefix          = "raw/aggtrade/"
    filename_suffix          = ".jsonl"
    filename_datetime_format = "YYYY/MM/DD/hh/mm_ssZ"
    max_duration             = "60s"
    max_bytes                = 10485760

    text_config {}
  }

  depends_on = [
    google_storage_bucket_iam_member.pubsub_bronze_object_creator,
    google_storage_bucket_iam_member.pubsub_bronze_bucket_reader,
  ]
}