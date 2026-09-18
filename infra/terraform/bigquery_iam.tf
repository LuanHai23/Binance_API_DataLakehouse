locals {
  terraform_bigquery_roles = toset([
    "roles/bigquery.connectionAdmin",
    "roles/bigquery.user",
  ])
}

resource "google_project_iam_member" "terraform_bigquery" {
  for_each = local.terraform_bigquery_roles

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${var.terraform_service_account}"

  depends_on = [
    google_project_service.workload["bigquery.googleapis.com"],
    google_project_service.workload["bigqueryconnection.googleapis.com"],
  ]
}
