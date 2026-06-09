import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

BRONZE_DEAD_LETTER_PATH = os.getenv(
    "BRONZE_DEAD_LETTER_PATH",
    "s3a://bronze/dead_letter/crypto_trades/"
)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("DeadLetterCheck")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Checking dead-letter zone...")
    print(f"Dead-letter path: {BRONZE_DEAD_LETTER_PATH}")

    try:
        df = spark.read.parquet(BRONZE_DEAD_LETTER_PATH)
        total_rows = df.count()

        print(f"Dead-letter rows: {total_rows}")

        if total_rows > 0:
            df.groupBy("error_reason").count().show(truncate=False)
            df.orderBy("dead_letter_processed_at", ascending=False).show(20, truncate=False)
        else:
            print("No dead-letter records found.")

    except Exception as e:
        print(f"No dead-letter data found yet or path does not exist: {e}")

    spark.stop()


if __name__ == "__main__":
    main()