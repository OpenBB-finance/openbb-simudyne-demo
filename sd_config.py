"""Environment-driven configuration and allowed-list validators."""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).parent
ASSETS_DIR = ROOT / "assets"

DEFAULT_SCENARIO_OPTIONS = ("flash_crash", "buy_panic", "normal", "trending_up")

BOOK_LEVELS = 10

INGESTION_SCHEMA_VERSION = 1


def _clean(value: str) -> str:
    """Strip whitespace and a single layer of surrounding quotes."""
    v = str(value or "").strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
        v = v[1:-1].strip()
    return v


def parse_csv_env(raw_value: str) -> list[str]:
    cleaned = _clean(raw_value)
    return [_clean(part) for part in cleaned.split(",") if _clean(part)]


def _flag(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() not in {"0", "false", "no", ""}


SIMUDYNE_ALLOWED_DATES = tuple(dict.fromkeys(parse_csv_env(os.getenv("SIMUDYNE_ALLOWED_DATES", ""))))
SIMUDYNE_ALLOWED_DATES_SET = set(SIMUDYNE_ALLOWED_DATES)

DEFAULT_SIMUDYNE_SYMBOL = _clean(os.getenv("SIMUDYNE_SYMBOL", ""))
SIMUDYNE_ALLOWED_SYMBOLS = tuple(dict.fromkeys(parse_csv_env(os.getenv("SIMUDYNE_ALLOWED_SYMBOLS", ""))))
SIMUDYNE_ALLOWED_SYMBOLS_SET = set(SIMUDYNE_ALLOWED_SYMBOLS)

SIMUDYNE_ALLOWED_SCENARIOS = tuple(
    dict.fromkeys(parse_csv_env(os.getenv("SIMUDYNE_ALLOWED_SCENARIOS", ",".join(DEFAULT_SCENARIO_OPTIONS))))
)
SIMUDYNE_ALLOWED_SCENARIOS_SET = set(SIMUDYNE_ALLOWED_SCENARIOS)

DEFAULT_SIMUDYNE_DATE = (SIMUDYNE_ALLOWED_DATES[0] if SIMUDYNE_ALLOWED_DATES else "") or _clean(os.getenv("SIMUDYNE_DATE", ""))

STARTUP_FORCE_REFRESH = _flag("SIMUDYNE_STARTUP_FORCE_REFRESH", "0")
STARTUP_BLOCKING_WARMUP = _flag("SIMUDYNE_STARTUP_BLOCKING_WARMUP", "1")
KEEP_RAW_PARQUET = _flag("SIMUDYNE_KEEP_RAW_PARQUET", "1")

SIMUDYNE_API_TIMEOUT_SEC = float(os.getenv("SIMUDYNE_API_TIMEOUT_SEC", "120"))
SIMUDYNE_API_RETRIES = max(0, int(os.getenv("SIMUDYNE_API_RETRIES", "2")))
SIMUDYNE_API_RETRY_BACKOFF_SEC = max(0.0, float(os.getenv("SIMUDYNE_API_RETRY_BACKOFF_SEC", "2.0")))

DATA_DIR = Path(os.getenv("SIMUDYNE_DATA_DIR", "/app/.simudyne_data"))
RAW_DIR = DATA_DIR / "raw"
DUCKDB_PATH = DATA_DIR / "stream.duckdb"

WARMUP_MAX_WORKERS = max(1, int(os.getenv("SIMUDYNE_WARMUP_WORKERS", "8")))
DUCKDB_THREADS = max(1, int(os.getenv("SIMUDYNE_DUCKDB_THREADS", "4")))


def ensure_allowed_date(date: str) -> str:
    value = str(date or "").strip()
    if SIMUDYNE_ALLOWED_DATES_SET and value not in SIMUDYNE_ALLOWED_DATES_SET:
        allowed = ", ".join(SIMUDYNE_ALLOWED_DATES)
        raise ValueError(f"Date '{value}' is not allowed. Set date to one of: {allowed}")
    return value


def ensure_allowed_symbol(symbol: str) -> str:
    value = str(symbol or "").strip()
    if SIMUDYNE_ALLOWED_SYMBOLS_SET and value not in SIMUDYNE_ALLOWED_SYMBOLS_SET:
        allowed = ", ".join(SIMUDYNE_ALLOWED_SYMBOLS)
        raise ValueError(f"Symbol '{value}' is not allowed. Set symbol to one of: {allowed}")
    return value


def ensure_allowed_scenario(scenario: str) -> str:
    value = str(scenario or "").strip()
    if SIMUDYNE_ALLOWED_SCENARIOS_SET and value not in SIMUDYNE_ALLOWED_SCENARIOS_SET:
        allowed = ", ".join(SIMUDYNE_ALLOWED_SCENARIOS)
        raise ValueError(f"Scenario '{value}' is not allowed. Set scenario to one of: {allowed}")
    return value
