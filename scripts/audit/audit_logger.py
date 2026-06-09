import os
import sys
import argparse
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "warehouse_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "adminpassword")


def parse_datetime(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def write_audit(args):
    started_at = parse_datetime(args.started_at)
    ended_at = parse_datetime(args.ended_at)

    duration_seconds = None
    if started_at and ended_at:
        duration_seconds = (ended_at - started_at).total_seconds()

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO public.pipeline_run_audit (
            dag_id,
            run_id,
            task_name,
            status,
            started_at,
            ended_at,
            duration_seconds,
            input_rows,
            output_rows,
            error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            args.dag_id,
            args.run_id,
            args.task_name,
            args.status,
            started_at,
            ended_at,
            duration_seconds,
            args.input_rows,
            args.output_rows,
            args.error_message,
        ),
    )

    conn.commit()
    cursor.close()
    conn.close()

    print(
        f"Audit logged: dag={args.dag_id}, "
        f"run={args.run_id}, task={args.task_name}, status={args.status}"
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--task-name", required=True)
    parser.add_argument("--status", required=True)

    parser.add_argument("--started-at", required=False)
    parser.add_argument("--ended-at", required=False)

    parser.add_argument("--input-rows", type=int, required=False)
    parser.add_argument("--output-rows", type=int, required=False)

    parser.add_argument("--error-message", required=False)

    args = parser.parse_args()

    try:
        write_audit(args)
    except Exception as e:
        print(f"Failed to write audit log: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()