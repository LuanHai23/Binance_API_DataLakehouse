from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping
import math

REQUIRED_FIELDS = {
    "e",
    "s",
    "a",
    "p",
    "q",
    "f",
    "l",
    "T",
    "m",
}

SCHEMA_VERSION = 1


def _to_utc_z(dt: datetime) -> str:

    if dt.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")

    return (
        dt.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _validate_decimal(
    value: Any,
    field_name: str,
) -> tuple[str, Decimal]:

    raw = str(value)

    try:
        decimal_value = Decimal(raw)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(
            f"{field_name} must be a valid decimal: {raw!r}"
        ) from exc

    if not decimal_value.is_finite():
        raise ValueError(
            f"{field_name} must be finite: {raw!r}"
        )

    if decimal_value <= 0:
        raise ValueError(
            f"{field_name} must be > 0: {raw!r}"
        )

    float_value = float(decimal_value)

    if not math.isfinite(float_value):
        raise ValueError(
            f"{field_name} is too large for float: {raw!r}"
        )

    return raw, decimal_value


def _validate_int(
    value: Any,
    field_name: str,
) -> int:
    """
    Validate a non-negative integer without coercion.
    """

    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"{field_name} must be a non-negative integer: {value!r}"
        )

    if value < 0:
        raise ValueError(
            f"{field_name} must be >= 0: {value!r}"
        )

    return value

def build_agg_trade_event(
    data: Mapping[str, Any],
    *,
    ingested_at: datetime | None = None,
) -> dict[str, Any]:

    missing = REQUIRED_FIELDS - set(data.keys())

    if missing:
        raise ValueError(
            f"Missing required Binance fields: {sorted(missing)}"
        )

    if data["e"] != "aggTrade":
        raise ValueError(
            f"Expected Binance event type 'aggTrade', got {data['e']!r}"
        )

    symbol = str(data["s"]).strip().upper()

    if not symbol:
        raise ValueError("symbol must not be empty")

    trade_id = _validate_int(data["a"], "trade_id")
    first_trade_id = _validate_int(
        data["f"],
        "first_trade_id",
    )
    last_trade_id = _validate_int(
        data["l"],
        "last_trade_id",
    )

    price_raw, price_decimal = _validate_decimal(
        data["p"],
        "price",
    )

    quantity_raw, quantity_decimal = _validate_decimal(
        data["q"],
        "quantity",
    )

    if not isinstance(data["m"], bool):
        raise ValueError(
            f"'m' must be bool, got {type(data['m']).__name__}"
        )

    is_buyer_maker = data["m"]

    trade_time = _validate_int(
        data["T"],
        "trade_time",
    )

    try:
        event_dt = datetime.fromtimestamp(
            trade_time / 1000,
            tz=timezone.utc,
        )
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError(
            f"Invalid trade timestamp: {trade_time!r}"
        ) from exc

    if ingested_at is None:
        ingested_at = datetime.now(timezone.utc)

    if ingested_at.tzinfo is None:
        raise ValueError(
            "ingested_at must be timezone-aware"
        )

    event_time = _to_utc_z(event_dt)
    ingested_at_iso = _to_utc_z(ingested_at)

    event_id = f"binance:aggTrade:{symbol}:{trade_id}"

    return {
        "event_id": event_id,
        "source": "binance_aggTrade",
        "event_type": "aggTrade",
        "schema_version": SCHEMA_VERSION,

        "symbol": symbol,
        "trade_id": trade_id,
        "first_trade_id": first_trade_id,
        "last_trade_id": last_trade_id,

        "price": float(price_decimal),
        "quantity": float(quantity_decimal),
        "trade_time": trade_time,

        "price_raw": price_raw,
        "quantity_raw": quantity_raw,

        "event_time": event_time,
        "ingested_at": ingested_at_iso,

        "is_buyer_maker": is_buyer_maker,
    }