import os
import sys
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

BRONZE_OUTPUT_PATH = os.getenv(
    "BRONZE_OUTPUT_PATH",
    "s3a://bronze/crypto_trades/"
)

EXPECTED_SYMBOLS = {
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "SHIBUSDT",
}


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BronzeDataQualityCheck")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def fail_if(condition, message):
    if condition:
        print(f"[FAILED] {message}")
        sys.exit(1)

    print(f"[PASSED] {message}")


def safe_sum_null(df, column_name):
    if column_name not in df.columns:
        print(f"[SKIPPED] Column not found: {column_name}")
        return 0

    return df.select(
        spark_sum(when(col(column_name).isNull(), 1).otherwise(0)).alias("cnt")
    ).collect()[0]["cnt"]


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Bronze data quality checks...")
    print(f"Bronze path: {BRONZE_OUTPUT_PATH}")

    bronze_data_path = BRONZE_OUTPUT_PATH.rstrip("/") + "/year=*/month=*/day=*/hour=*"

    df = (
        spark.read
        .option("basePath", BRONZE_OUTPUT_PATH)
        .parquet(bronze_data_path)
    )

    total_rows = df.count()
    print(f"Total Bronze rows: {total_rows}")

    fail_if(total_rows == 0, "Bronze table must not be empty")

    required_columns = [
        "symbol",
        "price",
        "quantity",
        "trade_timestamp",
    ]

    for column_name in required_columns:
        fail_if(column_name not in df.columns, f"Bronze must contain column: {column_name}")

    metrics = df.select(
        spark_sum(when(col("symbol").isNull(), 1).otherwise(0)).alias("null_symbol"),
        spark_sum(when(col("price").isNull(), 1).otherwise(0)).alias("null_price"),
        spark_sum(when(col("quantity").isNull(), 1).otherwise(0)).alias("null_quantity"),
        spark_sum(when(col("trade_timestamp").isNull(), 1).otherwise(0)).alias("null_trade_timestamp"),
        spark_sum(when(col("price") <= 0, 1).otherwise(0)).alias("invalid_price"),
        spark_sum(when(col("quantity") <= 0, 1).otherwise(0)).alias("invalid_quantity"),
    ).collect()[0].asDict()

    for key, value in metrics.items():
        print(f"{key}: {value}")

    fail_if(metrics["null_symbol"] > 0, "symbol must not be null")
    fail_if(metrics["null_price"] > 0, "price must not be null")
    fail_if(metrics["null_quantity"] > 0, "quantity must not be null")
    fail_if(metrics["null_trade_timestamp"] > 0, "trade_timestamp must not be null")
    fail_if(metrics["invalid_price"] > 0, "price must be > 0")
    fail_if(metrics["invalid_quantity"] > 0, "quantity must be > 0")

    actual_symbols = {row["symbol"] for row in df.select("symbol").distinct().collect()}
    unexpected_symbols = actual_symbols - EXPECTED_SYMBOLS

    print(f"Actual symbols: {sorted(actual_symbols)}")
    print(f"Unexpected symbols: {sorted(unexpected_symbols)}")

    fail_if(len(unexpected_symbols) > 0, "Bronze contains unexpected symbols")

    if "trade_id" in df.columns:
        null_trade_id = safe_sum_null(df, "trade_id")
        print(f"null_trade_id: {null_trade_id}")
        fail_if(null_trade_id > 0, "trade_id must not be null")

        duplicate_trade_id_count = (
            df.groupBy("symbol", "trade_id")
            .agg(count("*").alias("cnt"))
            .filter(col("cnt") > 1)
            .count()
        )

        print(f"duplicate symbol+trade_id count: {duplicate_trade_id_count}")
        fail_if(duplicate_trade_id_count > 0, "Bronze must not contain duplicate symbol+trade_id")

    if "raw_value" in df.columns:
        null_raw_value = safe_sum_null(df, "raw_value")
        print(f"null_raw_value: {null_raw_value}")
        fail_if(null_raw_value > 0, "raw_value must not be null")

    if "offset" in df.columns:
        null_offset = safe_sum_null(df, "offset")
        print(f"null_offset: {null_offset}")
        fail_if(null_offset > 0, "Kafka offset must not be null")

    print("Bronze data quality checks passed.")
    spark.stop()


if __name__ == "__main__":
    main()