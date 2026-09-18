locals {
  silver_aggtrade_schema = [
    {
      name = "event_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "source"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "event_type"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "schema_version"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "trade_id"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "first_trade_id"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "last_trade_id"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "price"
      type = "NUMERIC"
      mode = "NULLABLE"
    },
    {
      name = "quantity"
      type = "NUMERIC"
      mode = "NULLABLE"
    },
    {
      name = "price_raw"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "quantity_raw"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "trade_time"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "event_time"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "ingested_at"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "is_buyer_maker"
      type = "BOOLEAN"
      mode = "NULLABLE"
    },
    {
      name = "trade_side"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "trade_value"
      type = "NUMERIC"
      mode = "NULLABLE"
    },
    {
      name = "event_hour"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "source_file"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "silver_processed_at"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },

    # BigQuery exposes Hive partition keys in the catalog schema even
    # though these columns are absent from the Parquet payload.
    {
      name = "source_batch"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "event_date"
      type = "DATE"
      mode = "NULLABLE"
    },
    {
      name = "symbol"
      type = "STRING"
      mode = "NULLABLE"
    },
  ]
}

resource "google_bigquery_table" "silver_aggtrade" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.silver.dataset_id
  table_id   = "aggtrade"

  friendly_name = "Binance Silver aggTrade ${var.environment}"
  description   = "BigLake catalog table over curated Binance aggTrade Silver Parquet."

  deletion_protection = true
  resource_tags       = {}

  labels = {
    component  = "silver"
    data_layer = "silver"
    entity     = "aggtrade"
  }

  schema = jsonencode(local.silver_aggtrade_schema)

  external_data_configuration {
    autodetect    = false
    source_format = "PARQUET"

    # BigQuery canonicalizes the connection identifier into
    # PROJECT_NUMBER.REGION.CONNECTION_ID form when reading the table.
    connection_id = join(".", [
      data.google_project.binance_lakehouse.number,
      var.region,
      google_bigquery_connection.silver_biglake.connection_id,
    ])

    # Match the API defaults returned during Terraform refresh.
    decimal_target_types  = []
    ignore_unknown_values = false
    max_bad_records       = 0

    source_uris = [
      "gs://${google_storage_bucket.silver.name}/aggtrade/*",
    ]

    hive_partitioning_options {
      mode = "CUSTOM"

      source_uri_prefix = join("", [
        "gs://${google_storage_bucket.silver.name}/aggtrade/",
        "{source_batch:STRING}/",
        "{event_date:DATE}/",
        "{symbol:STRING}",
      ])

      require_partition_filter = true
    }
  }

  depends_on = [
    google_storage_bucket_iam_member.biglake_silver_reader,
  ]
}
