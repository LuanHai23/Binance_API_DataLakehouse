terraform {
  backend "gcs" {
    bucket                      = "binance-lakehouse-dev-1611-tfstate"
    prefix                      = "binance-lakehouse/dev"
    impersonate_service_account = "tf-binance-dev@binance-lakehouse-dev-1611.iam.gserviceaccount.com"
  }
}