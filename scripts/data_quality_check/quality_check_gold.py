import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

GOLD_OUTPUT_PATH = os.getenv("GOLD_OUTPUT_PATH", "s3a://gold/market_candles/")

def create_spark_session():
    return (
        SparkSession.builder
        .appName("GoldDataQualityCheck")
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

    print("Starting Gold Data Quality Check")
    print(f"Gold path: {GOLD_OUTPUT_PATH}")

    df = spark.read.parquet(GOLD_OUTPUT_PATH)

    total_rows = df.count()
    print(f"Total Gold rows: {total_rows}")

    fail_if(total_rows == 0, "Gold table must not be empty")

    metrics = df.select(
        spark_sum(when(col("symbol").isNull(), 1).otherwise(0)).alias("null_symbol"),
        spark_sum(when(col("candle_start_time").isNull(), 1).otherwise(0)).alias("null_candle_start_time"),
        spark_sum(when(col("open_price").isNull(), 1).otherwise(0)).alias("null_open_price"),
        spark_sum(when(col("high_price").isNull(), 1).otherwise(0)).alias("null_high_price"),
        spark_sum(when(col("low_price").isNull(), 1).otherwise(0)).alias("null_low_price"),
        spark_sum(when(col("close_price").isNull(), 1).otherwise(0)).alias("null_close_price"),
        spark_sum(when(col("high_price") < col("low_price"), 1).otherwise(0)).alias("invalid_high_low"),
        spark_sum(when(col("open_price") <= 0, 1).otherwise(0)).alias("invalid_open_price"),
        spark_sum(when(col("close_price") <= 0, 1).otherwise(0)).alias("invalid_close_price"),
        spark_sum(when(col("total_volume") < 0, 1).otherwise(0)).alias("invalid_volume"),
        spark_sum(when(col("trade_count") <= 0, 1).otherwise(0)).alias("invalid_trade_count"),
    ).collect()[0].asDict()

    for key, value in metrics.items():
        print(f"{key}: {value}")

    fail_if(metrics["null_symbol"] > 0, "symbol must not be null")
    fail_if(metrics["null_candle_start_time"] > 0, "candle_start_time must not be null")
    fail_if(metrics["null_open_price"] > 0, "open_price must not be null")
    fail_if(metrics["null_high_price"] > 0, "high_price must not be null")
    fail_if(metrics["null_low_price"] > 0, "low_price must not be null")
    fail_if(metrics["null_close_price"] > 0, "close_price must not be null")
    fail_if(metrics["invalid_high_low"] > 0, "high_price must be >= low_price")
    fail_if(metrics["invalid_open_price"] > 0, "open_price must be > 0")
    fail_if(metrics["invalid_close_price"] > 0, "close_price must be > 0")
    fail_if(metrics["invalid_volume"] > 0, "total_volume must be >= 0")
    fail_if(metrics["invalid_trade_count"] > 0, "trade_count must be > 0")

    duplicate_count = (
        df.groupBy("symbol", "candle_start_time")
        .agg(count("*").alias("cnt"))
        .filter(col("cnt") > 1)
        .count()
    )

    print(f"duplicate symbol+candle_start_time count: {duplicate_count}")
    fail_if(duplicate_count > 0, "Gold table must not contain duplicate symbol+candle_start_time")

    print("Gold data quality checks passed.")
    spark.stop()


if __name__ == "__main__":
    main()
