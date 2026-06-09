from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/project"

MINIO_CONF = {
    "endpoint": os.getenv("MINIO_ENDPOINT"),
    "access_key": os.getenv("MINIO_ACCESS_KEY"),
    "secret_key": os.getenv("MINIO_SECRET_KEY"),
}

POSTGRES_CONF = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "db": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

BRONZE_OUTPUT_PATH = os.getenv("BRONZE_OUTPUT_PATH")
SILVER_OUTPUT_PATH = os.getenv("SILVER_OUTPUT_PATH")
GOLD_OUTPUT_PATH = os.getenv("GOLD_OUTPUT_PATH")
BRONZE_CHECKPOINT_PATH = os.getenv("BRONZE_CHECKPOINT_PATH")
SILVER_CHECKPOINT_PATH = os.getenv("SILVER_CHECKPOINT_PATH")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
BRONZE_DEAD_LETTER_PATH = os.getenv("BRONZE_DEAD_LETTER_PATH")
BRONZE_DEAD_LETTER_CHECKPOINT_PATH = os.getenv("BRONZE_DEAD_LETTER_CHECKPOINT_PATH")





default_args = {
    "owner": "luan",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id = "binance_lakehouse_pipeline",
    default_args=default_args,
    description="A DAG to orchestrate the Binance Lakehouse Pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags = ["binance", "lakehouse", "spark", "dbt", "scd"]
) as dag:
    
    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"""
        docker exec \
        -e MINIO_ENDPOINT={MINIO_CONF['endpoint']} \
        -e MINIO_ACCESS_KEY={MINIO_CONF['access_key']} \
        -e MINIO_SECRET_KEY={MINIO_CONF['secret_key']} \
        -e KAFKA_BOOTSTRAP_SERVERS={KAFKA_BOOTSTRAP_SERVERS} \
        -e KAFKA_TOPIC={KAFKA_TOPIC} \
        -e BRONZE_OUTPUT_PATH={BRONZE_OUTPUT_PATH} \
        -e BRONZE_CHECKPOINT_PATH={BRONZE_CHECKPOINT_PATH} \
        -e BRONZE_DEAD_LETTER_PATH={BRONZE_DEAD_LETTER_PATH} \
        -e BRONZE_DEAD_LETTER_CHECKPOINT_PATH={BRONZE_DEAD_LETTER_CHECKPOINT_PATH} \
        de_spark_master spark-submit \
            --master spark://spark-master:7077 \
            --total-executor-cores 1 \
            --executor-memory 512m \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            /opt/bitnami/spark/scripts/Medallion/spark_stream_bronze_ingestion_data.py
        """,
    )

    dead_letter_check = BashOperator(
        task_id="dead_letter_check",
        bash_command=f"""
        docker exec \
        -e MINIO_ENDPOINT={MINIO_CONF['endpoint']} \
        -e MINIO_ACCESS_KEY={MINIO_CONF['access_key']} \
        -e MINIO_SECRET_KEY={MINIO_CONF['secret_key']} \
        -e BRONZE_DEAD_LETTER_PATH={BRONZE_DEAD_LETTER_PATH} \
        -e BRONZE_DEAD_LETTER_CHECKPOINT_PATH={BRONZE_DEAD_LETTER_CHECKPOINT_PATH} \
        de_spark_master spark-submit \
            --master spark://spark-master:7077 \
            --total-executor-cores 1 \
            --executor-memory 512m \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            /opt/bitnami/spark/scripts/data_quality_check/check_dead_letter.py
        """,
    )

    bronze_quality_check = BashOperator(
    task_id="bronze_quality_check",
        bash_command=f"""
        docker exec \
        -e MINIO_ENDPOINT={MINIO_CONF['endpoint']} \
        -e MINIO_ACCESS_KEY={MINIO_CONF['access_key']} \
        -e MINIO_SECRET_KEY={MINIO_CONF['secret_key']} \
        -e BRONZE_OUTPUT_PATH={BRONZE_OUTPUT_PATH} \
        de_spark_master spark-submit \
            --master spark://spark-master:7077 \
            --total-executor-cores 1 \
            --executor-memory 512m \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            /opt/bitnami/spark/scripts/data_quality_check/quality_check_bronze.py
        """,
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=f"""
        docker exec \
        -e MINIO_ENDPOINT={MINIO_CONF['endpoint']} \
        -e MINIO_ACCESS_KEY={MINIO_CONF['access_key']} \
        -e MINIO_SECRET_KEY={MINIO_CONF['secret_key']} \
        -e BRONZE_OUTPUT_PATH={BRONZE_OUTPUT_PATH} \
        -e SILVER_OUTPUT_PATH={SILVER_OUTPUT_PATH} \
        -e SILVER_CHECKPOINT_PATH={SILVER_CHECKPOINT_PATH} \
        de_spark_master spark-submit \
            --master spark://spark-master:7077 \
            --total-executor-cores 1 \
            --executor-memory 512m \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            /opt/bitnami/spark/scripts/Medallion/spark_stream_silver_transform_data.py
        """,
    )
    
    silver_quality_check = BashOperator(
        task_id="silver_quality_check",
        bash_command=f"""
        docker exec \
        -e MINIO_ENDPOINT={MINIO_CONF['endpoint']} \
        -e MINIO_ACCESS_KEY={MINIO_CONF['access_key']} \
        -e MINIO_SECRET_KEY={MINIO_CONF['secret_key']} \
        -e SILVER_OUTPUT_PATH={SILVER_OUTPUT_PATH} \
        de_spark_master spark-submit \
            --master spark://spark-master:7077 \
            --total-executor-cores 1 \
            --executor-memory 512m \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            /opt/bitnami/spark/scripts/data_quality_check/quality_check_silver.py
        """,
    )

    gold_aggregation = BashOperator(
        task_id="gold_aggregation",
        bash_command=f"""
        docker exec \
        -e MINIO_ENDPOINT={MINIO_CONF['endpoint']} \
        -e MINIO_ACCESS_KEY={MINIO_CONF['access_key']} \
        -e MINIO_SECRET_KEY={MINIO_CONF['secret_key']} \
        -e SILVER_OUTPUT_PATH={SILVER_OUTPUT_PATH} \
        -e GOLD_OUTPUT_PATH={GOLD_OUTPUT_PATH} \
        de_spark_master spark-submit \
            --master spark://spark-master:7077 \
            --total-executor-cores 1 \
            --executor-memory 512m \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            /opt/bitnami/spark/scripts/Medallion/spark_stream_gold_aggregate_modeling_data.py
        """,
    )
    gold_quality_check = BashOperator(
        task_id="gold_quality_check",
        bash_command=f"""
        docker exec \
        -e MINIO_ENDPOINT={MINIO_CONF['endpoint']} \
        -e MINIO_ACCESS_KEY={MINIO_CONF['access_key']} \
        -e MINIO_SECRET_KEY={MINIO_CONF['secret_key']} \
        -e GOLD_OUTPUT_PATH={GOLD_OUTPUT_PATH} \
        de_spark_master spark-submit \
            --master spark://spark-master:7077 \
            --total-executor-cores 1 \
            --executor-memory 512m \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            /opt/bitnami/spark/scripts/data_quality_check/quality_check_gold.py
        """,
    )

    load_gold_to_postgres = BashOperator(
        task_id="load_gold_to_postgres",
        bash_command=f"""
        docker exec \
        -e MINIO_ENDPOINT={MINIO_CONF['endpoint']} \
        -e MINIO_ACCESS_KEY={MINIO_CONF['access_key']} \
        -e MINIO_SECRET_KEY={MINIO_CONF['secret_key']} \
        -e GOLD_OUTPUT_PATH={GOLD_OUTPUT_PATH} \
        -e POSTGRES_HOST={POSTGRES_CONF['host']} \
        -e POSTGRES_PORT={POSTGRES_CONF['port']} \
        -e POSTGRES_DB={POSTGRES_CONF['db']} \
        -e POSTGRES_USER={POSTGRES_CONF['user']} \
        -e POSTGRES_PASSWORD={POSTGRES_CONF['password']} \
        de_spark_master spark-submit \
            --master spark://spark-master:7077 \
            --total-executor-cores 1 \
            --executor-memory 512m \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 \
            /opt/bitnami/spark/scripts/Medallion/load_gold_to_postgres.py

        docker cp /opt/project/scripts/init_db/upsert_fact_market_candles.sql de_postgres:/tmp/upsert_fact_market_candles.sql
        docker exec de_postgres psql -U admin -d warehouse_db -f /tmp/upsert_fact_market_candles.sql
        """,
    )

    load_symbol_metadata = BashOperator(
        task_id="load_symbol_metadata",
        bash_command=f"""
        cd {PROJECT_DIR} && python scripts/Medallion/load_symbol_metadata.py
        """,
        env={
            "POSTGRES_HOST": "{POSTGRES_CONF['host']}",
            "POSTGRES_PORT": "{POSTGRES_CONF['port']}",
            "POSTGRES_DB": "{POSTGRES_CONF['db']}",
            "POSTGRES_USER": "{POSTGRES_CONF['user']}",
            "POSTGRES_PASSWORD": "{POSTGRES_CONF['password']}",
        },
    )

    apply_scd_type2 = BashOperator(
        task_id="apply_scd_type2",
        bash_command=f"""
        docker cp {PROJECT_DIR}/scripts/init_db/apply_symbol_scd_type_2.sql de_postgres:/tmp/apply_symbol_scd_type_2.sql
        docker exec de_postgres psql -U admin -d warehouse_db -f /tmp/apply_symbol_scd_type_2.sql
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {PROJECT_DIR}/binance_analytics && dbt run --profiles-dir .
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd {PROJECT_DIR}/binance_analytics && dbt test --profiles-dir .
        """,
    )

    write_pipeline_audit_summary = BashOperator(
        task_id="write_pipeline_audit_summary",
        bash_command="""
        cd /opt/project && python scripts/audit/audit_logger.py \
        --dag-id "{{ dag.dag_id }}" \
        --run-id "{{ run_id }}" \
        --task-name "binance_lakehouse_pipeline" \
        --status "SUCCESS" \
        --started-at "{{ dag_run.start_date }}" \
        --ended-at "{{ ts }}"
        """,
        env={
            "POSTGRES_HOST": POSTGRES_CONF["host"],
            "POSTGRES_PORT": POSTGRES_CONF["port"],
            "POSTGRES_DB": POSTGRES_CONF["db"],
            "POSTGRES_USER": POSTGRES_CONF["user"],
            "POSTGRES_PASSWORD": POSTGRES_CONF["password"],
        },
    )

    bronze_ingestion >> dead_letter_check >> bronze_quality_check >> silver_transform >> silver_quality_check >> gold_aggregation >> gold_quality_check >> load_gold_to_postgres >> load_symbol_metadata >> apply_scd_type2 >> dbt_run >> dbt_test >> write_pipeline_audit_summary