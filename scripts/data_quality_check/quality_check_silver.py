import os
import sys
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

SILVER_OUTPUT_PATH = os.getenv(
    "SILVER_OUTPUT_PATH",
    "s3a://silver/crypto_trades/"
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
        .appName("SilverDataQualityCheck")
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


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    silver_data_path = SILVER_OUTPUT_PATH.rstrip("/") + "/year=*/month=*/day=*/hour=*"

    print("Starting Silver data quality checks...")
    print(f"Silver path: {SILVER_OUTPUT_PATH}")

    df = (
        spark.read
        .option("basePath", SILVER_OUTPUT_PATH)
        .parquet(silver_data_path)
    )

    total_rows = df.count()
    print(f"Total Silver rows: {total_rows}")

    fail_if(total_rows == 0, "Silver table must not be empty")

    metrics = df.select(
        spark_sum(when(col("symbol").isNull(), 1).otherwise(0)).alias("null_symbol"),
        spark_sum(when(col("trade_timestamp").isNull(), 1).otherwise(0)).alias("null_trade_timestamp"),
        spark_sum(when(col("price").isNull(), 1).otherwise(0)).alias("null_price"),
        spark_sum(when(col("quantity").isNull(), 1).otherwise(0)).alias("null_quantity"),
        spark_sum(when(col("trade_value").isNull(), 1).otherwise(0)).alias("null_trade_value"),
        spark_sum(when(col("price") <= 0, 1).otherwise(0)).alias("invalid_price"),
        spark_sum(when(col("quantity") <= 0, 1).otherwise(0)).alias("invalid_quantity"),
        spark_sum(when(col("trade_value") <= 0, 1).otherwise(0)).alias("invalid_trade_value"),
    ).collect()[0].asDict()

    for key, value in metrics.items():
        print(f"{key}: {value}")

    fail_if(metrics["null_symbol"] > 0, "symbol must not be null")
    fail_if(metrics["null_trade_timestamp"] > 0, "trade_timestamp must not be null")
    fail_if(metrics["null_price"] > 0, "price must not be null")
    fail_if(metrics["null_quantity"] > 0, "quantity must not be null")
    fail_if(metrics["null_trade_value"] > 0, "trade_value must not be null")
    fail_if(metrics["invalid_price"] > 0, "price must be > 0")
    fail_if(metrics["invalid_quantity"] > 0, "quantity must be > 0")
    fail_if(metrics["invalid_trade_value"] > 0, "trade_value must be > 0")

    actual_symbols = {row["symbol"] for row in df.select("symbol").distinct().collect()}
    unexpected_symbols = actual_symbols - EXPECTED_SYMBOLS

    print(f"Actual symbols: {sorted(actual_symbols)}")
    print(f"Unexpected symbols: {sorted(unexpected_symbols)}")

    fail_if(len(unexpected_symbols) > 0, "Silver contains unexpected symbols")

    if "trade_id" in df.columns:
        duplicate_count = (
            df.groupBy("symbol", "trade_id")
            .agg(count("*").alias("cnt"))
            .filter(col("cnt") > 1)
            .count()
        )

        print(f"duplicate symbol+trade_id count: {duplicate_count}")
        fail_if(duplicate_count > 0, "Silver must not contain duplicate symbol+trade_id")

    print("Silver data quality checks passed.")
    spark.stop()


if __name__ == "__main__":
    main()