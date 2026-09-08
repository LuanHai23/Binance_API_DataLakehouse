resource "google_billing_budget" "pipeline_monthly" {
  billing_account = "01A175-D742A7-006DE4"
  display_name    = "binance-${var.environment}-monthly-guardrail"

  # Prevent accidental deletion of the financial guardrail.
  deletion_policy = "PREVENT"

  budget_filter {
    calendar_period        = "MONTH"
    credit_types_treatment = "EXCLUDE_ALL_CREDITS"

    projects = [
      "projects/${data.google_project.binance_lakehouse.number}",
    ]
  }

  amount {
    specified_amount {
      currency_code = "VND"
      units         = "500000"
    }
  }

  # Preserve the ordering returned by the live Budget API.
  threshold_rules {
    threshold_percent = 0.5
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 0.9
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 0.1
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 0.25
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 0.75
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "FORECASTED_SPEND"
  }

  depends_on = [
    google_project_iam_member.terraform_budget_manager,
    google_project_service.workload["billingbudgets.googleapis.com"],
  ]
}
