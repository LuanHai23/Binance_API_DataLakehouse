import argparse
import re

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    DecimalType,
)


DECIMAL_TYPE = DecimalType(38, 18)
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"
BATCH_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("schema_version", IntegerType(), True),
    StructField("symbol", StringType(), True),
    StructField("trade_id", LongType(), True),
    StructField("first_trade_id", LongType(), True),
    StructField("last_trade_id", LongType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("trade_time", LongType(), True),
    StructField("price_raw", StringType(), True),
    StructField("quantity_raw", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("is_buyer_maker", BooleanType(), True),
    StructField("_corrupt_record", StringType(), True),
])


def parse_gcs_uri(value: str) -> str:
    normalized = value.strip().rstrip("/")

    if not normalized.startswith("gs://"):
        raise argparse.ArgumentTypeError(
            "URI must start with gs://"
        )

    return normalized


def parse_batch_id(value: str) -> str:
    normalized = value.strip()

    if not BATCH_ID_PATTERN.fullmatch(normalized):
        raise argparse.ArgumentTypeError(
            "batch ID may contain only letters, numbers, "
            "underscores, and hyphens"
        )

    return normalized


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Transform Binance Bronze JSONL into Silver Parquet."
    )

    parser.add_argument(
        "--input-uri",
        required=True,
        type=parse_gcs_uri,
    )
    parser.add_argument(
        "--silver-base-uri",
        required=True,
        type=parse_gcs_uri,
    )
    parser.add_argument(
        "--batch-id",
        required=True,
        type=parse_batch_id,
    )

    return parser.parse_args()


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BinanceSilverBatch")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def add_validation_columns(bronze_df: DataFrame) -> DataFrame:
    expected_event_id = F.concat(
        F.lit("binance:aggTrade:"),
        F.col("symbol"),
        F.lit(":"),
        F.col("trade_id").cast("string"),
    )

    enriched_df = (
        bronze_df
        .withColumn("_source_file", F.input_file_name())
        .withColumn(
            "_price_decimal",
            F.col("price_raw").cast(DECIMAL_TYPE),
        )
        .withColumn(
            "_quantity_decimal",
            F.col("quantity_raw").cast(DECIMAL_TYPE),
        )
        .withColumn(
            "_event_timestamp",
            F.to_timestamp(
                F.col("event_time"),
                TIMESTAMP_FORMAT,
            ),
        )
        .withColumn(
            "_ingested_timestamp",
            F.to_timestamp(
                F.col("ingested_at"),
                TIMESTAMP_FORMAT,
            ),
        )
        .withColumn("_expected_event_id", expected_event_id)
    )

    return enriched_df.withColumn(
        "error_reason",
        F.when(
            F.col("_corrupt_record").isNotNull(),
            F.lit("MALFORMED_JSON"),
        )
        .when(
            F.col("event_id").isNull()
            | (F.trim(F.col("event_id")) == ""),
            F.lit("EVENT_ID_INVALID"),
        )
        .when(
            F.col("source").isNull()
            | (F.col("source") != "binance_aggTrade"),
            F.lit("SOURCE_INVALID"),
        )
        .when(
            F.col("event_type").isNull()
            | (F.col("event_type") != "aggTrade"),
            F.lit("EVENT_TYPE_INVALID"),
        )
        .when(
            F.col("schema_version").isNull()
            | (F.col("schema_version") != 1),
            F.lit("SCHEMA_VERSION_INVALID"),
        )
        .when(
            F.col("symbol").isNull()
            | ~F.col("symbol").rlike(r"^[A-Z0-9]{5,20}$"),
            F.lit("SYMBOL_INVALID"),
        )
        .when(
            F.col("trade_id").isNull()
            | (F.col("trade_id") < 0),
            F.lit("TRADE_ID_INVALID"),
        )
        .when(
            F.col("first_trade_id").isNull()
            | F.col("last_trade_id").isNull()
            | (F.col("first_trade_id") < 0)
            | (F.col("last_trade_id") < F.col("first_trade_id")),
            F.lit("TRADE_RANGE_INVALID"),
        )
        .when(
            F.col("trade_time").isNull()
            | (F.col("trade_time") < 0),
            F.lit("TRADE_TIME_INVALID"),
        )
        .when(
            F.col("event_id") != F.col("_expected_event_id"),
            F.lit("EVENT_ID_MISMATCH"),
        )
        .when(
            F.col("price").isNull()
            | F.col("_price_decimal").isNull()
            | (F.col("_price_decimal") <= 0),
            F.lit("PRICE_INVALID"),
        )
        .when(
            F.col("quantity").isNull()
            | F.col("_quantity_decimal").isNull()
            | (F.col("_quantity_decimal") <= 0),
            F.lit("QUANTITY_INVALID"),
        )
        .when(
            F.col("_event_timestamp").isNull(),
            F.lit("EVENT_TIME_INVALID"),
        )
        .when(
            F.col("_ingested_timestamp").isNull(),
            F.lit("INGESTED_AT_INVALID"),
        )
        .when(
            F.col("is_buyer_maker").isNull(),
            F.lit("BUYER_MAKER_INVALID"),
        )
    )


def build_silver_dataframe(validated_df: DataFrame) -> DataFrame:
    return (
        validated_df
        .filter(F.col("error_reason").isNull())
        .dropDuplicates(["event_id"])
        .select(
            "event_id",
            "source",
            "event_type",
            "schema_version",
            "symbol",
            "trade_id",
            "first_trade_id",
            "last_trade_id",
            F.col("_price_decimal").alias("price"),
            F.col("_quantity_decimal").alias("quantity"),
            "price_raw",
            "quantity_raw",
            "trade_time",
            F.col("_event_timestamp").alias("event_time"),
            F.col("_ingested_timestamp").alias("ingested_at"),
            "is_buyer_maker",
            F.when(
                F.col("is_buyer_maker") == F.lit(False),
                F.lit("BUY"),
            )
            .otherwise(F.lit("SELL"))
            .alias("trade_side"),
            (
                F.col("_price_decimal")
                * F.col("_quantity_decimal")
            )
            .cast(DECIMAL_TYPE)
            .alias("trade_value"),
            F.to_date(
                F.col("_event_timestamp")
            ).alias("event_date"),
            F.hour(
                F.col("_event_timestamp")
            ).alias("event_hour"),
            F.col("_source_file").alias("source_file"),
            F.current_timestamp().alias("silver_processed_at"),
        )
    )


def main() -> None:
    args = parse_arguments()
    spark = create_spark_session()
    validated_df = None

    silver_output_uri = (
        f"{args.silver_base_uri}/aggtrade/"
        f"source_batch={args.batch_id}"
    )
    quarantine_output_uri = (
        f"{args.silver_base_uri}/quarantine/aggtrade/"
        f"source_batch={args.batch_id}"
    )

    try:
        spark.sparkContext.setLogLevel("WARN")

        bronze_df = (
            spark.read
            .option(
                "columnNameOfCorruptRecord",
                "_corrupt_record",
            )
            .schema(BRONZE_SCHEMA)
            .json(args.input_uri)
        )

        validated_df = add_validation_columns(
            bronze_df
        ).persist(StorageLevel.MEMORY_AND_DISK)

        input_rows = validated_df.count()

        if input_rows == 0:
            raise RuntimeError(
                "Bronze input contains no records"
            )

        rejected_df = validated_df.filter(
            F.col("error_reason").isNotNull()
        )

        valid_before_dedup = validated_df.filter(
            F.col("error_reason").isNull()
        ).count()

        rejected_rows = rejected_df.count()
        silver_df = build_silver_dataframe(validated_df)
        silver_rows = silver_df.count()
        duplicate_rows = valid_before_dedup - silver_rows

        print(f"Input URI: {args.input_uri}")
        print(f"Silver output URI: {silver_output_uri}")
        print(f"Quarantine URI: {quarantine_output_uri}")
        print(f"Input rows: {input_rows}")
        print(f"Rejected rows: {rejected_rows}")
        print(f"Duplicate rows removed: {duplicate_rows}")
        print(f"Silver rows: {silver_rows}")

        (
            rejected_df.write
            .mode("overwrite")
            .parquet(quarantine_output_uri)
        )

        if silver_rows == 0:
            raise RuntimeError(
                "No valid Silver records were produced"
            )

        (
            silver_df
            .repartition("event_date", "symbol")
            .write
            .mode("overwrite")
            .partitionBy("event_date", "symbol")
            .parquet(silver_output_uri)
        )
        written_metrics = (
            spark.read
            .parquet(silver_output_uri)
            .agg(
                F.count("*").alias("row_count"),
                F.countDistinct("event_id").alias(
                    "unique_event_ids"
                ),
            )
            .first()
        )

        written_silver_rows = int(
            written_metrics["row_count"]
        )
        written_unique_event_ids = int(
            written_metrics["unique_event_ids"]
        )

        written_quarantine_rows = (
            spark.read
            .parquet(quarantine_output_uri)
            .count()
        )

        print(
            f"Written Silver rows: "
            f"{written_silver_rows}"
        )
        print(
            f"Written unique event IDs: "
            f"{written_unique_event_ids}"
        )
        print(
            f"Written quarantine rows: "
            f"{written_quarantine_rows}"
        )

        if written_silver_rows != silver_rows:
            raise RuntimeError(
                "Silver read-back row count mismatch"
            )

        if written_unique_event_ids != written_silver_rows:
            raise RuntimeError(
                "Duplicate event IDs detected after write"
            )

        if written_quarantine_rows != rejected_rows:
            raise RuntimeError(
                "Quarantine read-back row count mismatch"
            )
        print("Silver batch completed successfully")

    finally:
        if validated_df is not None:
            validated_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()