import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    hour,
    when,
    round,
    log,
    lit,
    current_timestamp
)

load_dotenv()

MINIO_CONF = {
    "endpoint": os.getenv("MINIO_ENDPOINT"),
    "access_key": os.getenv("MINIO_ACCESS_KEY"),
    "secret_key": os.getenv("MINIO_SECRET_KEY")
}

BRONZE_INPUT_PATH = os.getenv(
    "BRONZE_OUTPUT_PATH",
    "s3a://bronze/crypto_trades/"
)

SILVER_OUTPUT_PATH = os.getenv(
    "SILVER_OUTPUT_PATH",
    "s3a://silver/crypto_trades/"
)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BinanceSilverTransformBatch")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Silver Transform Batch Job Started...")
    print(f"Bronze input path: {BRONZE_INPUT_PATH}")
    print(f"Silver output path: {SILVER_OUTPUT_PATH}")

    bronze_data_path = BRONZE_INPUT_PATH.rstrip("/") + "/year=*/month=*/day=*/hour=*"

    print(f"Reading Bronze data from: {bronze_data_path}")

    bronze_df = (
        spark.read
        .option("basePath", BRONZE_INPUT_PATH)
        .parquet(bronze_data_path)
    )

    print(f"Bronze rows: {bronze_df.count()}")

    cleaned_df = (
        bronze_df
        .filter(col("symbol").isNotNull())
        .filter(col("trade_id").isNotNull())
        .filter(col("trade_timestamp").isNotNull())
        .filter(col("price").isNotNull() & (col("price") > 0))
        .filter(col("quantity").isNotNull() & (col("quantity") > 0))
        .filter(col("price") < 1_000_000)
    )

    transformed_df = (
        cleaned_df
        .withColumn("price", round(col("price"), 8))
        .withColumn("quantity", round(col("quantity"), 8))
        .withColumn("ingest_timestamp", col("ingested_at").cast("timestamp"))
    )

    enriched_df = (
        transformed_df
        .withColumn("trade_value", round(col("price") * col("quantity"), 8))
        .withColumn(
            "trade_side",
            when(col("is_buyer_maker") == False, lit("BUY"))
            .otherwise(lit("SELL"))
        )
        .withColumn(
            "price_magnitude",
            round(log(col("price")) / log(lit(10.0)), 2)
        )
        .withColumn(
            "is_large_trade",
            when(col("price") * col("quantity") > 10000, lit(True))
            .otherwise(lit(False))
        )
        .withColumn("silver_processed_at", current_timestamp())
    )

    deduped_df = (
        enriched_df
        .dropDuplicates(["symbol", "trade_id"])
    )

    final_df = (
        deduped_df
        .drop("year", "month", "day", "hour")
        .withColumn("year", year(col("trade_timestamp")))
        .withColumn("month", month(col("trade_timestamp")))
        .withColumn("day", dayofmonth(col("trade_timestamp")))
        .withColumn("hour", hour(col("trade_timestamp")))
    )

    print("Final Silver schema:")
    final_df.printSchema()

    print(f"Final Silver rows: {final_df.count()}")

    (
        final_df.write
        .mode("overwrite")
        .partitionBy("year", "month", "day", "hour")
        .parquet(SILVER_OUTPUT_PATH)
    )

    print("Silver transform complete — data written to MinIO/silver")

    spark.stop()


if __name__ == "__main__":
    main()