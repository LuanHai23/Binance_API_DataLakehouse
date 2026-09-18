resource "google_bigquery_table" "gold_mart_market_overview" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id   = "mart_market_overview"

  friendly_name = "Binance market overview ${var.environment}"
  description   = "BI-facing 24-hour market overview by symbol, evaluated relative to the latest Gold candle watermark."

  deletion_policy     = "PREVENT"
  deletion_protection = true
  resource_tags       = {}

  labels = {
    component     = "gold"
    data_layer    = "gold"
    serving_layer = "bi"
    model         = "market-overview"
  }

  view {
    query = templatefile(
      "${path.module}/../../sql/gcp_gold/mart_market_overview.sql.tftpl",
      {
        project_id   = var.project_id
        gold_dataset = google_bigquery_dataset.gold.dataset_id
      }
    )

    use_legacy_sql = false
  }

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    google_bigquery_table.gold_fact_market_candles_1m,
  ]
}
