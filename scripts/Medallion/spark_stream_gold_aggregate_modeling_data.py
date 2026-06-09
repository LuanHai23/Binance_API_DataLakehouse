import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    first,
    last,
    max,
    min,
    sum,
    count,
    when,
    lit,
    current_timestamp,
    year,
    month,
    dayofmonth
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
    TimestampType,
    IntegerType
)

load_dotenv()

MINIO_CONF = {
    "endpoint": os.getenv("MINIO_ENDPOINT"),
    "access_key": os.getenv("MINIO_ACCESS_KEY"),
    "secret_key": os.getenv("MINIO_SECRET_KEY")
}

SILVER_INPUT_PATH = os.getenv(
    "SILVER_OUTPUT_PATH",
    "s3a://silver/crypto_trades/"
)

GOLD_OUTPUT_PATH = os.getenv(
    "GOLD_OUTPUT_PATH",
    "s3a://gold/market_candles/"
)

GOLD_CHECKPOINT_PATH = os.getenv(
    "GOLD_CHECKPOINT_PATH",
    "s3a://gold/_checkpoints/market_candles/"
)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BinanceGoldOHLC")
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

    print("Gold Aggregation Job Started...")
    print(f"Silver input path: {SILVER_INPUT_PATH}")
    print(f"Gold output path: {GOLD_OUTPUT_PATH}")

    silver_schema = StructType([
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("message_key", StringType(), True),
        StructField("raw_value", StringType(), True),

        StructField("source", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("trade_id", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("first_trade_id", LongType(), True),
        StructField("last_trade_id", LongType(), True),
        StructField("trade_time", LongType(), True),
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("ingested_at", StringType(), True),

        StructField("trade_timestamp", TimestampType(), True),
        StructField("bronze_processed_at", TimestampType(), True),

        StructField("ingest_timestamp", TimestampType(), True),
        StructField("trade_value", DoubleType(), True),
        StructField("trade_side", StringType(), True),
        StructField("price_magnitude", DoubleType(), True),
        StructField("is_large_trade", BooleanType(), True),
        StructField("silver_processed_at", TimestampType(), True),

        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
    ])

    silver_df = (
        spark.read
        .schema(silver_schema)
        .parquet(SILVER_INPUT_PATH)
    )

    valid_trades_df = (
        silver_df
        .filter(col("symbol").isNotNull())
        .filter(col("trade_timestamp").isNotNull())
        .filter(col("price").isNotNull() & (col("price") > 0))
        .filter(col("quantity").isNotNull() & (col("quantity") > 0))
    )

    gold_df = (
        valid_trades_df
        #.withWatermark("trade_timestamp", "10 minutes")
        .groupBy(
            col("symbol"),
            window(col("trade_timestamp"), "1 minute")
        )
        .agg(
            first("price", ignorenulls=True).alias("open_price"),
            max("price").alias("high_price"),
            min("price").alias("low_price"),
            last("price", ignorenulls=True).alias("close_price"),
            sum("quantity").alias("total_volume"),
            sum("trade_value").alias("quote_volume"),
            count("*").alias("trade_count"),
            sum(
                when(col("trade_side") == "BUY", col("quantity"))
                .otherwise(lit(0.0))
            ).alias("buy_volume_taker"),
            sum(
                when(col("trade_side") == "SELL", col("quantity"))
                .otherwise(lit(0.0))
            ).alias("sell_volume_maker"),
            sum(
                when(col("is_large_trade") == True, lit(1))
                .otherwise(lit(0))
            ).alias("large_trade_count")
        )
        .select(
            col("symbol"),
            col("window.start").alias("candle_start_time"),
            col("window.end").alias("candle_end_time"),
            col("open_price"),
            col("high_price"),
            col("low_price"),
            col("close_price"),
            col("total_volume"),
            col("quote_volume"),
            col("trade_count"),
            col("buy_volume_taker"),
            col("sell_volume_maker"),
            col("large_trade_count")
        )
        .withColumn("gold_processed_at", current_timestamp())
        .withColumn("year", year(col("candle_start_time")))
        .withColumn("month", month(col("candle_start_time")))
        .withColumn("day", dayofmonth(col("candle_start_time")))
    )

    gold_df.printSchema()

    query = (
        gold_df.write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .save(GOLD_OUTPUT_PATH)
    )

    print("Gold aggregation complete — data written to MinIO/gold")

if __name__ == "__main__":
    main()