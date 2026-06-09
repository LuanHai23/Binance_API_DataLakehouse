import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    year,
    month,
    when,
    lit,
    dayofmonth,
    hour,
    from_unixtime,
    current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType


load_dotenv()

MINIO_CONF = {
    "endpoint": os.getenv("MINIO_ENDPOINT"),
    "access_key": os.getenv("MINIO_ACCESS_KEY"),
    "secret_key": os.getenv("MINIO_SECRET_KEY")
}

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:29092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "crypto_trade_price_1"
)

BRONZE_OUTPUT_PATH = os.getenv(
    "BRONZE_OUTPUT_PATH",
    "s3a://bronze/crypto_trades/"
)

BRONZE_CHECKPOINT_PATH = os.getenv(
    "BRONZE_CHECKPOINT_PATH",
    "s3a://bronze/_checkpoints/crypto_trades/"
)

BRONZE_DEAD_LETTER_PATH = os.getenv(
    "BRONZE_DEAD_LETTER_PATH",
    "s3a://bronze/dead_letter/crypto_trades/"
)

BRONZE_DEAD_LETTER_CHECKPOINT_PATH = os.getenv(
    "BRONZE_DEAD_LETTER_CHECKPOINT_PATH",
    "s3a://bronze/_checkpoints/dead_letter/crypto_trades/"
)

def create_spark_session():
    return (
        SparkSession.builder
        .appName("CryptoBronzeIngestion")
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

    print("Bronze Ingestion Job Started...")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"Bronze output path: {BRONZE_OUTPUT_PATH}")

    schema = StructType([
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
        StructField("ingested_at", StringType(), True)
    ])

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("key").cast("string").alias("message_key"),
            col("value").cast("string").alias("raw_value"),
            from_json(col("value").cast("string"), schema).alias("data")
        )
        .select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "message_key",
            "raw_value",
            "data.*"
        )
    )

    validated_df = (
        parsed_df
        .withColumn(
            "error_reason",
            when(col("raw_value").isNull(), lit("RAW_VALUE_NULL"))
            .when(col("symbol").isNull(), lit("SYMBOL_NULL"))
            .when(col("trade_id").isNull(), lit("TRADE_ID_NULL"))
            .when(col("price").isNull(), lit("PRICE_NULL"))
            .when(col("quantity").isNull(), lit("QUANTITY_NULL"))
            .when(col("trade_time").isNull(), lit("TRADE_TIME_NULL"))
            .when(col("price") <= 0, lit("PRICE_INVALID"))
            .when(col("quantity") <= 0, lit("QUANTITY_INVALID"))
            .otherwise(lit(None))
        )
        .withColumn(
            "trade_timestamp",
            from_unixtime(col("trade_time") / 1000).cast("timestamp")
        )
        .withColumn("bronze_processed_at", current_timestamp())
        .withColumn("year", year(col("trade_timestamp")))
        .withColumn("month", month(col("trade_timestamp")))
        .withColumn("day", dayofmonth(col("trade_timestamp")))
        .withColumn("hour", hour(col("trade_timestamp")))
    )

    valid_df = (
        validated_df
        .filter(col("error_reason").isNull())
        .drop("error_reason")
    )

    dead_letter_df = (
        validated_df
        .filter(col("error_reason").isNotNull())
        .select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "message_key",
            "raw_value",
            "error_reason",
            current_timestamp().alias("dead_letter_processed_at")
        )
    )

    valid_df.printSchema()

    valid_query = (
        valid_df.writeStream
        .format("parquet")
        .option("path", BRONZE_OUTPUT_PATH)
        .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
        .partitionBy("year", "month", "day", "hour")
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )

    def write_dead_letter_batch(batch_df, batch_id):
        invalid_count = batch_df.count()

        print(f"Dead-letter batch_id={batch_id}, invalid_count={invalid_count}")

        if invalid_count > 0:
            (
                batch_df.write
                .mode("append")
                .parquet(BRONZE_DEAD_LETTER_PATH)
            )


    dead_letter_query = (
        dead_letter_df.writeStream
        .foreachBatch(write_dead_letter_batch)
        .option("checkpointLocation", BRONZE_DEAD_LETTER_CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )

    valid_query.awaitTermination()
    dead_letter_query.awaitTermination()

    print("Bronze ingestion complete — data written to MinIO/bronze")

if __name__ == "__main__":
    main()