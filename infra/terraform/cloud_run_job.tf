locals {
  producer_image_uri = "asia-southeast1-docker.pkg.dev/binance-lakehouse-dev-1611/binance-producer-dev/binance-producer@sha256:dae122b17ad06347a5e55db954d81f7a68d1c4ce62fb0be6e164a2c2f610d2cd"
}

resource "google_cloud_run_v2_job" "binance_ingestor" {
  project             = var.project_id
  name                = "binance-ingestor-dev"
  location            = var.region
  deletion_protection = false

  template {
    task_count  = 1
    parallelism = 1

    template {
      service_account = google_service_account.binance_ingestor.email
      max_retries     = 0
      timeout         = "3600s"

      containers {
        image = local.producer_image_uri

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }

        env {
          name  = "PUBLISH_BACKEND"
          value = "pubsub"
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }

        env {
          name  = "PUBSUB_TOPIC_ID"
          value = google_pubsub_topic.binance_aggtrade_events.name
        }

        env {
          name  = "RUN_DURATION_SECONDS"
          value = "3300"
        }
      }
    }
  }

  depends_on = [
    google_project_service.workload["run.googleapis.com"],
    google_artifact_registry_repository.binance_producer,
    google_pubsub_topic_iam_member.binance_ingestor_publisher,
  ]
}