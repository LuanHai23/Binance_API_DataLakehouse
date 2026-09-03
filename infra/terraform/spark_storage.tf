resource "google_storage_bucket" "silver" {
  project  = var.project_id
  name     = "${var.project_id}-silver"
  location = upper(var.region)

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle_rule {
    condition {
      age        = 30
      with_state = "ANY"
    }

    action {
      type = "Delete"
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_storage_bucket" "spark_staging" {
  project  = var.project_id
  name     = "${var.project_id}-spark-staging"
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