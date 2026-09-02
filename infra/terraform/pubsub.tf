resource "google_pubsub_topic" "binance_aggtrade_events" {
  project = var.project_id
  name    = "binance-aggtrade-events-${var.environment}"

  message_storage_policy {
    allowed_persistence_regions = [
      var.region,
    ]
  }

  depends_on = [
    google_project_service.workload["pubsub.googleapis.com"],
  ]
}

resource "google_pubsub_topic_iam_member" "binance_ingestor_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.binance_aggtrade_events.name
  role    = "roles/pubsub.publisher"
  member  = google_service_account.binance_ingestor.member
}

resource "google_pubsub_subscription" "binance_aggtrade_debug" {
  project = var.project_id
  name    = "binance-aggtrade-debug-dev"
  topic   = google_pubsub_topic.binance_aggtrade_events.id

  ack_deadline_seconds       = 30
  message_retention_duration = "3600s"
  retain_acked_messages      = false

  enable_exactly_once_delivery = false
  enable_message_ordering      = false

  expiration_policy {
    ttl = "86400s"
  }

  depends_on = [
    google_project_service.workload["pubsub.googleapis.com"],
  ]
}