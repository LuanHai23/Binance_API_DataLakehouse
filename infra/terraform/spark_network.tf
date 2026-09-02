locals {
  spark_subnet_cidr = "10.20.0.0/24"
}

resource "google_compute_network" "binance_data" {
  project                 = var.project_id
  name                    = "binance-data-${var.environment}"
  description             = "Dedicated VPC for Binance lakehouse data workloads"
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
  mtu                     = 1460

  depends_on = [
    google_project_service.workload["compute.googleapis.com"],
  ]
}

resource "google_compute_subnetwork" "binance_spark" {
  project                  = var.project_id
  name                     = "binance-spark-${var.environment}"
  description              = "Regional subnet for Managed Service for Apache Spark"
  region                   = var.region
  network                  = google_compute_network.binance_data.id
  ip_cidr_range            = local.spark_subnet_cidr
  private_ip_google_access = true
}

resource "google_compute_firewall" "spark_internal" {
  project     = var.project_id
  name        = "binance-spark-internal-${var.environment}"
  description = "Allow internal communication between Spark VMs in the dedicated subnet"
  network     = google_compute_network.binance_data.name
  direction   = "INGRESS"
  priority    = 1000

  source_ranges      = [local.spark_subnet_cidr]
  destination_ranges = [local.spark_subnet_cidr]

  allow {
    protocol = "all"
  }
}
