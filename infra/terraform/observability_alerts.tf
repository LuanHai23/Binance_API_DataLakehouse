variable "pipeline_alert_email" {
  description = "Email address that receives pipeline alerts"
  type        = string
  sensitive   = true

  validation {
    condition = can(
      regex(
        "^[^@]+@[^@]+[.][^@]+$",
        trimspace(var.pipeline_alert_email),
      )
    )

    error_message = "pipeline_alert_email must be a valid email address."
  }
}

resource "google_monitoring_notification_channel" "pipeline_email" {
  project      = var.project_id
  display_name = "Binance Lakehouse ${var.environment} pipeline alerts"
  type         = "email"
  enabled      = true

  labels = {
    email_address = var.pipeline_alert_email
  }

  depends_on = [
    google_project_service.workload["monitoring.googleapis.com"],
  ]
}

resource "google_monitoring_alert_policy" "ingestor_execution_failed" {
  project      = var.project_id
  display_name = "Binance Ingestor ${var.environment} - execution failed"
  combiner     = "OR"
  enabled      = true

  notification_channels = [
    google_monitoring_notification_channel.pipeline_email.name,
  ]

  user_labels = {
    application = "binance-lakehouse"
    component   = "ingestor"
    environment = var.environment
    severity    = "critical"
  }

  conditions {
    display_name = "Cloud Run ingestion execution did not succeed"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"cloud_run_job\"",
        "resource.labels.job_name = \"${google_cloud_run_v2_job.binance_ingestor.name}\"",
        "resource.labels.location = \"${var.region}\"",
        "metric.type = \"run.googleapis.com/job/completed_execution_count\"",
        "metric.labels.result != \"succeeded\"",
      ])

      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }

  documentation {
    mime_type = "text/markdown"

    content = <<-EOT
      The hourly Cloud Run ingestion job completed with a non-success result.

      Check the latest `binance-ingestor-dev` execution, container logs,
      Pub/Sub publishing errors, and the Bronze delivery path before retrying.
    EOT
  }
}

resource "google_monitoring_alert_policy" "silver_workflow_failed" {
  project      = var.project_id
  display_name = "Binance Silver ${var.environment} - workflow failed"
  combiner     = "OR"
  enabled      = true

  notification_channels = [
    google_monitoring_notification_channel.pipeline_email.name,
  ]

  user_labels = {
    application = "binance-lakehouse"
    component   = "silver"
    environment = var.environment
    severity    = "critical"
  }

  conditions {
    display_name = "Silver Workflow execution did not succeed"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"workflows.googleapis.com/Workflow\"",
        "resource.labels.workflow_id = \"${google_workflows_workflow.silver_batch.name}\"",
        "resource.labels.location = \"${var.region}\"",
        "metric.type = \"workflows.googleapis.com/finished_execution_count\"",
        "metric.labels.status != \"SUCCEEDED\"",
      ])

      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }

  documentation {
    mime_type = "text/markdown"

    content = <<-EOT
      The hourly Silver Workflow completed with a non-success status.

      Inspect the `binance-silver-dev` execution and its Dataproc batch.
      Verify Bronze input availability, Spark driver logs, IAM, and output metrics.
    EOT
  }
}