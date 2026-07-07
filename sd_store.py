"""DuckDB storage layer: schema, connection, and read queries."""

from __future__ import annotations

import json
import threading
import time

import duckdb

import sd_config as cfg


def _book_frame_columns() -> list[str]:
    cols: list[str] = []
    for lvl in range(1, cfg.BOOK_LEVELS + 1):
        cols.append(f"bid_size_{lvl}")
        cols.append(f"ask_size_{lvl}")
    for lvl in range(2, cfg.BOOK_LEVELS + 1):
        cols.append(f"bid_price_{lvl}")
        cols.append(f"ask_price_{lvl}")
    return cols


BOOK_FRAME_COLUMNS = _book_frame_columns()

FRAME_SCALAR_COLUMNS = [
    "nbbo_bid",
    "nbbo_ask",
    "market_price",
    "market_price_min",
    "market_price_max",
    "spread",
    "trade_count",
    "trade_volume",
    "trade_vwap",
    "last_trade_price",
    "runs_reporting",
    "day_open",
    "day_high",
    "day_low",
    "day_close",
    "day_volume",
]

_INT_FRAME_COLUMNS = {"trade_count", "runs_reporting"}

FRAME_OUTPUT_COLUMNS = ["time", "run", *FRAME_SCALAR_COLUMNS, *BOOK_FRAME_COLUMNS]

_con: duckdb.DuckDBPyConnection | None = None
_lock = threading.RLock()


def _frame_column_ddl() -> str:
    cols = [
        "symbol VARCHAR",
        "date VARCHAR",
        "scenario VARCHAR",
        "run VARCHAR",
        "frame_index INTEGER",
        "time VARCHAR",
    ]
    for name in FRAME_SCALAR_COLUMNS:
        col_type = "BIGINT" if name in _INT_FRAME_COLUMNS else "DOUBLE"
        cols.append(f"{name} {col_type}")
    for name in BOOK_FRAME_COLUMNS:
        cols.append(f"{name} DOUBLE")
    return ",\n  ".join(cols)


def get_con() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        with _lock:
            if _con is None:
                cfg.DATA_DIR.mkdir(parents=True, exist_ok=True)
                con = duckdb.connect(str(cfg.DUCKDB_PATH))
                con.execute(f"PRAGMA threads={cfg.DUCKDB_THREADS}")
                _init_schema(con)
                _con = con
    return _con


def _init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS frames (
          {_frame_column_ddl()},
          PRIMARY KEY (symbol, date, scenario, run, frame_index)
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
          symbol VARCHAR,
          date VARCHAR,
          scenario VARCHAR,
          run VARCHAR,
          bucket VARCHAR,
          seq BIGINT,
          time VARCHAR,
          side VARCHAR,
          price DOUBLE,
          size DOUBLE,
          order_id VARCHAR,
          trade_count BIGINT,
          trade_volume DOUBLE,
          trade_vwap DOUBLE
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS run_stats (
          symbol VARCHAR,
          date VARCHAR,
          scenario VARCHAR,
          run VARCHAR,
          mode VARCHAR,
          n_runs_available INTEGER,
          runs_included INTEGER,
          frame_count BIGINT,
          trade_rows BIGINT,
          total_trade_volume DOUBLE,
          avg_spread DOUBLE,
          min_market_price DOUBLE,
          max_market_price DOUBLE,
          total_messages DOUBLE,
          add_events DOUBLE,
          cancel_events DOUBLE,
          trade_events DOUBLE,
          cancellation_rate DOUBLE,
          cancel_to_add_rate DOUBLE,
          buy_notional DOUBLE,
          sell_notional DOUBLE,
          total_notional DOUBLE,
          notional_imbalance DOUBLE,
          PRIMARY KEY (symbol, date, scenario, run)
        );
        """
    )
    con.execute("CREATE TABLE IF NOT EXISTS kv (key VARCHAR PRIMARY KEY, value JSON);")
    con.execute(
        "CREATE INDEX IF NOT EXISTS trades_key ON trades (symbol, date, scenario, run, bucket);"
    )


def execute(sql: str, params: list | None = None):
    con = get_con()
    with _lock:
        return con.execute(sql, params) if params is not None else con.execute(sql)


def checkpoint() -> None:
    """Fold the WAL into stream.duckdb so the build is durable and inspectable."""
    con = get_con()
    with _lock:
        con.execute("CHECKPOINT")


def close() -> None:
    global _con
    with _lock:
        if _con is not None:
            try:
                _con.execute("CHECKPOINT")
            except Exception:
                pass
            _con.close()
            _con = None


def reset_all() -> None:
    """Drop all materialised data and the raw parquet, for a forced refresh."""
    import shutil

    con = get_con()
    with _lock:
        # Drop and recreate (rather than DELETE) so schema changes — e.g. order_id
        # BIGINT -> VARCHAR — take effect on an existing persisted database/volume.
        for table in ("frames", "trades", "run_stats", "kv"):
            con.execute(f"DROP TABLE IF EXISTS {table}")
        _init_schema(con)
    if cfg.RAW_DIR.exists():
        shutil.rmtree(cfg.RAW_DIR, ignore_errors=True)


def _rows_to_dicts(cur) -> list[dict]:
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def kv_set(key: str, value) -> None:
    con = get_con()
    with _lock:
        con.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", [key, json.dumps(value)]
        )


def kv_get(key: str):
    con = get_con()
    with _lock:
        row = con.execute("SELECT value FROM kv WHERE key = ?", [key]).fetchone()
    if not row or row[0] is None:
        return None
    return json.loads(row[0])


MANIFEST_KEY = "ingestion_manifest"


def write_manifest(plan: list[tuple[str, str, str]], runs_by_scenario: dict[str, int]) -> None:
    step_keys = [f"{symbol}:{date}:{scenario}" for symbol, date, scenario in plan]
    kv_set(
        MANIFEST_KEY,
        {
            "schema": cfg.INGESTION_SCHEMA_VERSION,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "allowed_dates": list(cfg.SIMUDYNE_ALLOWED_DATES),
            "allowed_symbols": list(cfg.SIMUDYNE_ALLOWED_SYMBOLS),
            "allowed_scenarios": list(cfg.SIMUDYNE_ALLOWED_SCENARIOS),
            "step_keys": step_keys,
            "runs_by_scenario": runs_by_scenario,
            "total_steps": len(step_keys),
        },
    )


def is_prebuilt_complete() -> tuple[bool, int, int, str]:
    manifest = kv_get(MANIFEST_KEY)
    if not isinstance(manifest, dict):
        return False, 0, 0, "missing_manifest"
    if int(manifest.get("schema") or 0) != cfg.INGESTION_SCHEMA_VERSION:
        return False, 0, 0, "schema_mismatch"
    if [str(v) for v in (manifest.get("allowed_dates") or [])] != list(cfg.SIMUDYNE_ALLOWED_DATES):
        return False, 0, 0, "allowed_dates_changed"
    if [str(v) for v in (manifest.get("allowed_symbols") or [])] != list(cfg.SIMUDYNE_ALLOWED_SYMBOLS):
        return False, 0, 0, "allowed_symbols_changed"
    if [str(v) for v in (manifest.get("allowed_scenarios") or [])] != list(cfg.SIMUDYNE_ALLOWED_SCENARIOS):
        return False, 0, 0, "allowed_scenarios_changed"

    step_keys = [str(v) for v in (manifest.get("step_keys") or [])]
    if not step_keys:
        return False, 0, 0, "empty_manifest"

    checked = 0
    for key in step_keys:
        checked += 1
        symbol, date, scenario = key.split(":", 2)
        present = execute(
            "SELECT count(*) FROM run_stats WHERE symbol=? AND date=? AND scenario=? AND run='all'",
            [symbol, date, scenario],
        ).fetchone()[0]
        if not present:
            return False, checked, len(step_keys), "missing_aggregate"
    return True, len(step_keys), len(step_keys), "ready"


def get_run_stats(symbol: str, date: str, scenario: str, run: str) -> dict | None:
    cur = execute(
        "SELECT * FROM run_stats WHERE symbol=? AND date=? AND scenario=? AND run=?",
        [symbol, date, scenario, run],
    )
    rows = _rows_to_dicts(cur)
    return rows[0] if rows else None


def get_nframes(symbol: str, date: str, scenario: str, run: str) -> int:
    return int(
        execute(
            "SELECT count(*) FROM frames WHERE symbol=? AND date=? AND scenario=? AND run=?",
            [symbol, date, scenario, run],
        ).fetchone()[0]
    )


def get_frames(symbol: str, date: str, scenario: str, run: str) -> list[dict]:
    cols = ", ".join(FRAME_OUTPUT_COLUMNS)
    cur = execute(
        f"SELECT {cols} FROM frames WHERE symbol=? AND date=? AND scenario=? AND run=? ORDER BY frame_index",
        [symbol, date, scenario, run],
    )
    return _rows_to_dicts(cur)


def get_frame(symbol: str, date: str, scenario: str, run: str, frame_index: int) -> dict | None:
    cols = ", ".join(FRAME_OUTPUT_COLUMNS)
    cur = execute(
        f"SELECT {cols} FROM frames WHERE symbol=? AND date=? AND scenario=? AND run=? AND frame_index=?",
        [symbol, date, scenario, run, frame_index],
    )
    rows = _rows_to_dicts(cur)
    return rows[0] if rows else None


def get_trades_by_second(symbol: str, date: str, scenario: str, run: str) -> dict[str, list[dict]]:
    cur = execute(
        """
        SELECT bucket, time, side, price, size, order_id, trade_count, trade_volume, trade_vwap
        FROM trades
        WHERE symbol=? AND date=? AND scenario=? AND run=?
        ORDER BY bucket, seq
        """,
        [symbol, date, scenario, run],
    )
    out: dict[str, list[dict]] = {}
    for row in _rows_to_dicts(cur):
        bucket = row.pop("bucket")
        out.setdefault(bucket, []).append({k: v for k, v in row.items() if v is not None})
    return out


def get_trades_for_second(symbol: str, date: str, scenario: str, run: str, bucket: str) -> list[dict]:
    """Trades for a single second — the per-tick lookup used by the websocket."""
    cur = execute(
        """
        SELECT time, side, price, size, order_id, trade_count, trade_volume, trade_vwap
        FROM trades
        WHERE symbol=? AND date=? AND scenario=? AND run=? AND bucket=?
        ORDER BY seq
        """,
        [symbol, date, scenario, run, bucket],
    )
    return [{k: v for k, v in row.items() if v is not None} for row in _rows_to_dicts(cur)]


def get_trade_rows(symbol: str, date: str, scenario: str, run: str) -> list[dict]:
    by_second = get_trades_by_second(symbol, date, scenario, run)
    return [tr for trades in by_second.values() for tr in trades]


STATS_METRIC_DEFS = [
    ("Frames", "frame_count", 0),
    ("Trade rows", "trade_rows", 0),
    ("Total trade volume", "total_trade_volume", 0),
    ("Avg spread", "avg_spread", 4),
    ("Min market price", "min_market_price", 4),
    ("Max market price", "max_market_price", 4),
    ("Total messages", "total_messages", 0),
    ("Add events", "add_events", 0),
    ("Cancel events", "cancel_events", 0),
    ("Trade events", "trade_events", 0),
    ("Cancellation rate", "cancellation_rate", 4),
    ("Cancel/Add rate", "cancel_to_add_rate", 4),
    ("Buy-side notional", "buy_notional", 2),
    ("Sell-side notional", "sell_notional", 2),
    ("Total notional", "total_notional", 2),
    ("Notional imbalance", "notional_imbalance", 4),
]


def build_stats_rows(run_stats: dict, scenario_stats: dict) -> list[dict]:
    rows: list[dict] = []
    for label, key, digits in STATS_METRIC_DEFS:
        run_value = float(run_stats.get(key) or 0.0)
        scenario_value = float(scenario_stats.get(key) or 0.0)
        delta_value = run_value - scenario_value
        if digits == 0:
            run_display = int(round(run_value))
            scenario_display = int(round(scenario_value))
            delta_display = int(round(delta_value))
        else:
            run_display = round(run_value, digits)
            scenario_display = round(scenario_value, digits)
            delta_display = round(delta_value, digits)
        rows.append(
            {"kpi": label, "loaded_run": run_display, "scenario_avg": scenario_display, "delta": delta_display}
        )
    return rows


def get_stats_rows(symbol: str, date: str, scenario: str, run: str) -> list[dict] | None:
    run_row = get_run_stats(symbol, date, scenario, run)
    scenario_row = get_run_stats(symbol, date, scenario, "all")
    if not run_row or not scenario_row:
        return None
    return build_stats_rows(run_row, scenario_row)


def _stats_dict(symbol: str, date: str, scenario: str, run: str, row: dict) -> dict:
    keys = [
        "frame_count", "trade_rows", "total_trade_volume", "avg_spread", "min_market_price",
        "max_market_price", "total_messages", "add_events", "cancel_events", "trade_events",
        "cancellation_rate", "cancel_to_add_rate", "buy_notional", "sell_notional",
        "total_notional", "notional_imbalance",
    ]
    stats = {k: row.get(k) for k in keys}
    stats.update(
        {
            "scenario": scenario,
            "run_label": run,
            "runs_included": row.get("runs_included"),
            "n_runs_available": row.get("n_runs_available"),
            "symbol": symbol,
            "date": date,
        }
    )
    return stats


def summary_markdown(stats: dict) -> str:
    lines = [
        "# Simudyne Market Stream (DuckDB)",
        "",
        f"- Scenario: {stats['scenario']}",
        f"- Run: {stats['run_label']}",
        f"- Runs included: {stats['runs_included']}",
        f"- Frames: {stats['frame_count']}",
        f"- Trade rows: {stats['trade_rows']}",
        f"- Total trade volume: {stats['total_trade_volume']}",
        f"- Average spread: {float(stats['avg_spread'] or 0.0):.4f}",
        f"- Market price range: {float(stats['min_market_price'] or 0.0):.4f} "
        f"to {float(stats['max_market_price'] or 0.0):.4f}",
    ]
    return "\n".join(lines)


def get_payload(symbol: str, date: str, scenario: str, run: str) -> dict | None:
    row = get_run_stats(symbol, date, scenario, run)
    if not row:
        return None
    stats = _stats_dict(symbol, date, scenario, run, row)
    # First/last frame wall-clock, so the player can label the timeline endpoints.
    bounds = execute(
        "SELECT min(time), max(time) FROM frames WHERE symbol=? AND date=? AND scenario=? AND run=?",
        [symbol, date, scenario, run],
    ).fetchone()
    meta = {
        "symbol": symbol,
        "date": date,
        "scenario": scenario,
        "run": run,
        "mode": row.get("mode"),
        "n_runs_available": row.get("n_runs_available"),
        "runs_included": row.get("runs_included"),
        "start_time": bounds[0] if bounds else None,
        "end_time": bounds[1] if bounds else None,
    }
    return {"meta": meta, "stats": stats, "summary_markdown": summary_markdown(stats)}


def distinct_symbols() -> list[str]:
    return [r[0] for r in execute("SELECT DISTINCT symbol FROM run_stats ORDER BY symbol").fetchall()]


def distinct_dates(symbol: str) -> list[str]:
    return [
        r[0]
        for r in execute(
            "SELECT DISTINCT date FROM run_stats WHERE symbol=? ORDER BY date", [symbol]
        ).fetchall()
    ]


def distinct_scenarios(symbol: str, date: str) -> list[str]:
    return [
        r[0]
        for r in execute(
            "SELECT DISTINCT scenario FROM run_stats WHERE symbol=? AND date=? ORDER BY scenario",
            [symbol, date],
        ).fetchall()
    ]


def distinct_runs(symbol: str, date: str, scenario: str) -> list[str]:
    rows = execute(
        "SELECT DISTINCT run FROM run_stats WHERE symbol=? AND date=? AND scenario=?",
        [symbol, date, scenario],
    ).fetchall()
    runs = {str(r[0]) for r in rows}
    numeric = sorted((r for r in runs if r != "all"), key=lambda v: int(v) if v.isdigit() else 10**9)
    if "all" in runs:
        numeric.append("all")
    return numeric
