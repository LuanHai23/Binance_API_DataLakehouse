variable "project_id" {
  description = "GCP project ID for the Binance lakehouse development environment."
  type        = string
  default     = "binance-lakehouse-dev-1611"

  validation {
    condition     = var.project_id == "binance-lakehouse-dev-1611"
    error_message = "This stack is restricted to the Binance development project."
  }
}

variable "region" {
  description = "Primary GCP region for regional resources."
  type        = string
  default     = "asia-southeast1"

  validation {
    condition     = var.region == "asia-southeast1"
    error_message = "This stack must remain in asia-southeast1 to avoid cross-region cost."
  }
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "dev"
}

variable "terraform_service_account" {
  description = "Service account impersonated by the Terraform Google provider."
  type        = string
  default     = "tf-binance-dev@binance-lakehouse-dev-1611.iam.gserviceaccount.com"
}