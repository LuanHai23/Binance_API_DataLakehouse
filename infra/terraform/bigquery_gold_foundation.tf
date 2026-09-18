locals {
  gold_fact_market_candles_1m_schema = [
    {
      name        = "symbol"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Binance trading pair."
    },
    {
      name        = "candle_date"
      type        = "DATE"
      mode        = "REQUIRED"
      description = "UTC date of the one-minute candle start."
    },
    {
      name        = "candle_start_time"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Inclusive UTC boundary of the one-minute candle."
    },
    {
      name        = "candle_end_time"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Exclusive UTC boundary of the one-minute candle."
    },
    {
      name        = "open_price"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "First deterministically ordered trade price in the candle."
    },
    {
      name        = "high_price"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Maximum trade price in the candle."
    },
    {
      name        = "low_price"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Minimum trade price in the candle."
    },
    {
      name        = "close_price"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Last deterministically ordered trade price in the candle."
    },
    {
      name        = "total_volume"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Total base-asset quantity traded in the candle."
    },
    {
      name        = "quote_volume"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Total quote-asset trade value in the candle."
    },
    {
      name        = "trade_count"
      type        = "INTEGER"
      mode        = "REQUIRED"
      description = "Number of accepted Silver trades in the candle."
    },
    {
      name        = "buy_volume_taker"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Base-asset quantity classified as taker buy volume."
    },
    {
      name        = "sell_volume_maker"
      type        = "NUMERIC"
      mode        = "REQUIRED"
      description = "Base-asset quantity classified as maker sell volume."
    },
    {
      name        = "large_trade_count"
      type        = "INTEGER"
      mode        = "REQUIRED"
      description = "Trades whose quote value is greater than 10000 USDT."
    },
    {
      name        = "source_batch"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Silver source batch that most recently produced the candle."
    },
    {
      name        = "source_min_event_time"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Earliest source event timestamp represented in the candle."
    },
    {
      name        = "source_max_event_time"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Latest source event timestamp represented in the candle."
    },
    {
      name        = "gold_processed_at"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Timestamp when the Gold aggregation was evaluated."
    },
    {
      name        = "loaded_at"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Timestamp when the target candle was inserted or updated."
    },
  ]
}

resource "google_bigquery_dataset" "gold" {
  project    = var.project_id
  dataset_id = "binance_gold_${var.environment}"

  friendly_name = "Binance Gold ${var.environment}"
  description   = "Native BigQuery analytical models derived from curated Binance Silver data."
  location      = var.region

  deletion_policy            = "PREVENT"
  delete_contents_on_destroy = false

  labels = {
    component  = "gold"
    data_layer = "gold"
  }

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    google_project_iam_member.terraform_bigquery["roles/bigquery.user"],
  ]
}

resource "google_bigquery_table" "gold_fact_market_candles_1m" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id   = "fact_market_candles_1m"

  friendly_name = "Binance Gold one-minute market candles ${var.environment}"
  description   = "Deterministic one-minute OHLCV candles derived from the Silver aggTrade catalog."

  deletion_policy     = "PREVENT"
  deletion_protection = true
  resource_tags       = {}

  schema = jsonencode(local.gold_fact_market_candles_1m_schema)

  time_partitioning {
    type  = "DAY"
    field = "candle_date"
  }

  require_partition_filter = true
  clustering               = ["symbol"]

  labels = {
    component  = "gold"
    data_layer = "gold"
  }
}
