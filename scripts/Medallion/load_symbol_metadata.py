import hashlib
import os
from datetime import datetime

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

BINANCE_EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"

SYMBOLS = {
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "SHIBUSDT",
}

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT"))
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


def get_filter_value(filters, filter_type, key, default=None):
    for item in filters:
        if item.get("filterType") == filter_type:
            value = item.get(key, default)
            if value is None:
                return default
            try:
                return float(value)
            except ValueError:
                return default
    return default


def build_record_hash(record: dict) -> str:
    hash_input = "|".join(
        str(record.get(key, "")) for key in [
            "symbol",
            "base_asset",
            "quote_asset",
            "status",
            "price_precision",
            "quantity_precision",
            "tick_size",
            "step_size",
            "min_qty",
        ]
    )
    return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()


def fetch_symbol_metadata():
    response = requests.get(BINANCE_EXCHANGE_INFO_URL, timeout=30)
    response.raise_for_status()

    exchange_info = response.json()
    records = []

    for symbol_info in exchange_info.get("symbols", []):
        symbol = symbol_info.get("symbol")

        if symbol not in SYMBOLS:
            continue

        filters = symbol_info.get("filters", [])

        record = {
            "symbol": symbol,
            "base_asset": symbol_info.get("baseAsset"),
            "quote_asset": symbol_info.get("quoteAsset"),
            "status": symbol_info.get("status"),
            "price_precision": symbol_info.get("pricePrecision"),
            "quantity_precision": symbol_info.get("quantityPrecision"),
            "tick_size": get_filter_value(filters, "PRICE_FILTER", "tickSize"),
            "step_size": get_filter_value(filters, "LOT_SIZE", "stepSize"),
            "min_qty": get_filter_value(filters, "LOT_SIZE", "minQty"),
        }

        record["record_hash"] = build_record_hash(record)
        records.append(record)

    return records


def upsert_staging(records):
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    cursor = conn.cursor()

    for record in records:
        cursor.execute(
            """
            INSERT INTO public.stg_symbol_metadata (
                symbol,
                base_asset,
                quote_asset,
                status,
                price_precision,
                quantity_precision,
                tick_size,
                step_size,
                min_qty,
                record_hash,
                extracted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE SET
                base_asset = EXCLUDED.base_asset,
                quote_asset = EXCLUDED.quote_asset,
                status = EXCLUDED.status,
                price_precision = EXCLUDED.price_precision,
                quantity_precision = EXCLUDED.quantity_precision,
                tick_size = EXCLUDED.tick_size,
                step_size = EXCLUDED.step_size,
                min_qty = EXCLUDED.min_qty,
                record_hash = EXCLUDED.record_hash,
                extracted_at = EXCLUDED.extracted_at
            """,
            (
                record["symbol"],
                record["base_asset"],
                record["quote_asset"],
                record["status"],
                record["price_precision"],
                record["quantity_precision"],
                record["tick_size"],
                record["step_size"],
                record["min_qty"],
                record["record_hash"],
                datetime.utcnow(),
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()


def main():
    print("Fetching Binance symbol metadata...")
    records = fetch_symbol_metadata()

    print(f"Fetched {len(records)} symbol records.")
    upsert_staging(records)

    print("Symbol metadata loaded into stg_symbol_metadata.")


if __name__ == "__main__":
    main()