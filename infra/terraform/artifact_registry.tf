resource "google_artifact_registry_repository" "binance_producer" {
  project       = var.project_id
  location      = var.region
  repository_id = "binance-producer-dev"
  description   = "Docker images for the Binance ingestion workload"
  format        = "DOCKER"

  cleanup_policy_dry_run = true

  docker_config {
    immutable_tags = false
  }

  cleanup_policies {
    id     = "delete-older-than-seven-days"
    action = "DELETE"

    condition {
      tag_state  = "ANY"
      older_than = "604800s"
    }
  }

  cleanup_policies {
    id     = "keep-five-most-recent"
    action = "KEEP"

    most_recent_versions {
      keep_count = 5
    }
  }

  depends_on = [
    google_project_service.workload[
      "artifactregistry.googleapis.com"
    ],
  ]
}