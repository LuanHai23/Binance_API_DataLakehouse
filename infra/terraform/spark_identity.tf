locals {
  dataproc_service_agent = format(
    "service-%s@dataproc-accounts.iam.gserviceaccount.com",
    data.google_project.binance_lakehouse.number,
  )

  compute_service_agent = format(
    "service-%s@compute-system.iam.gserviceaccount.com",
    data.google_project.binance_lakehouse.number,
  )
}

resource "google_project_iam_custom_role" "spark_worker_restricted" {
  project     = var.project_id
  role_id     = "binanceSparkWorker"
  title       = "Binance Spark Worker"
  description = "Dataproc Worker permissions without project-wide Storage object access."
  stage       = "GA"

  permissions = [
    "cloudprofiler.profiles.create",
    "cloudprofiler.profiles.update",
    "datalineage.locations.processOpenLineageMessage",
    "dataproc.agents.create",
    "dataproc.agents.delete",
    "dataproc.agents.get",
    "dataproc.agents.list",
    "dataproc.agents.update",
    "dataproc.batches.computeTuningConfig",
    "dataproc.batches.sparkApplicationWrite",
    "dataproc.sessions.sparkApplicationWrite",
    "dataproc.tasks.lease",
    "dataproc.tasks.listInvalidatedLeases",
    "dataproc.tasks.reportStatus",
    "dataprocrm.nodePools.create",
    "dataprocrm.nodePools.delete",
    "dataprocrm.nodePools.deleteNodes",
    "dataprocrm.nodePools.get",
    "dataprocrm.nodePools.list",
    "dataprocrm.nodePools.resize",
    "dataprocrm.nodes.get",
    "dataprocrm.nodes.heartbeat",
    "dataprocrm.nodes.list",
    "dataprocrm.nodes.mintOAuthToken",
    "dataprocrm.operations.get",
    "logging.logEntries.create",
    "logging.logEntries.route",
    "monitoring.metricDescriptors.create",
    "monitoring.metricDescriptors.get",
    "monitoring.metricDescriptors.list",
    "monitoring.monitoredResourceDescriptors.get",
    "monitoring.monitoredResourceDescriptors.list",
    "monitoring.timeSeries.create",
    "storage.buckets.get",
  ]

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_service_account" "binance_spark" {
  project      = var.project_id
  account_id   = "binance-spark-${var.environment}"
  display_name = "Binance Spark - ${var.environment}"
  description  = "Runtime identity for Binance Managed Service for Apache Spark workloads"

  deletion_policy = "PREVENT"
}

resource "google_project_iam_member" "spark_runtime_worker" {
  project = var.project_id
  role    = google_project_iam_custom_role.spark_worker_restricted.name
  member  = "serviceAccount:${google_service_account.binance_spark.email}"
}

resource "google_service_account_iam_member" "terraform_can_act_as_spark" {
  service_account_id = google_service_account.binance_spark.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.terraform_service_account}"
}

resource "google_service_account_iam_member" "dataproc_agent_can_act_as_spark" {
  service_account_id = google_service_account.binance_spark.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${local.dataproc_service_agent}"
}

resource "google_service_account_iam_member" "dataproc_agent_can_mint_spark_tokens" {
  service_account_id = google_service_account.binance_spark.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${local.dataproc_service_agent}"
}

resource "google_service_account_iam_member" "compute_agent_can_mint_spark_tokens" {
  service_account_id = google_service_account.binance_spark.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${local.compute_service_agent}"
}

resource "google_storage_bucket_iam_member" "spark_bronze_reader" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.binance_spark.email}"
}

resource "google_storage_bucket_iam_member" "spark_silver_writer" {
  bucket = google_storage_bucket.silver.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.binance_spark.email}"
}

resource "google_storage_bucket_iam_member" "spark_staging_writer" {
  bucket = google_storage_bucket.spark_staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.binance_spark.email}"
}

resource "google_storage_bucket_iam_member" "spark_code_reader" {
  bucket = google_storage_bucket.spark_code.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.binance_spark.email}"
}
