resource "google_project_iam_custom_role" "terraform_budget_manager" {
  project = var.project_id
  role_id = "binanceBudgetManager"
  title   = "Binance Budget Manager"

  description = "Manages budgets scoped only to the Binance Lakehouse project."

  permissions = [
    "billing.resourcebudgets.read",
    "billing.resourcebudgets.write",
    "resourcemanager.projects.get",
  ]
}

resource "google_project_iam_member" "terraform_budget_manager" {
  project = var.project_id
  role    = google_project_iam_custom_role.terraform_budget_manager.name
  member  = "serviceAccount:${var.terraform_service_account}"
}
