import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

load_dotenv()

MINIO_CONF = {
    "endpoint": os.getenv("MINIO_ENDPOINT"),
    "access_key": os.getenv("MINIO_ACCESS_KEY"),
    "secret_key": os.getenv("MINIO_SECRET_KEY"),
}

GOLD_INPUT_PATH = os.getenv(
    "GOLD_OUTPUT_PATH",
    "s3a://gold/market_candles/"
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def create_spark_session():
    return (
        SparkSession.builder
        .appName("LoadGoldToPostgres")
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

    print("Load Gold to PostgreSQL job started...")
    print(f"Gold input path: {GOLD_INPUT_PATH}")
    print(f"PostgreSQL JDBC URL: {JDBC_URL}")

    gold_df = spark.read.parquet(GOLD_INPUT_PATH)

    final_df = (
        gold_df
        .select(
            col("symbol"),
            col("candle_start_time"),
            col("candle_end_time"),
            col("open_price"),
            col("high_price"),
            col("low_price"),
            col("close_price"),
            col("total_volume"),
            col("quote_volume"),
            col("trade_count"),
            col("buy_volume_taker"),
            col("sell_volume_maker"),
            col("large_trade_count"),
            col("gold_processed_at"),
        )
        .withColumn("loaded_at", current_timestamp())
    )

    print("Gold rows to load:", final_df.count())
    final_df.printSchema()
    final_df.show(20, truncate=False)

    (
        final_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "public.stg_market_candles")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    print("Gold data loaded to PostgreSQL successfully.")


if __name__ == "__main__":
    main()