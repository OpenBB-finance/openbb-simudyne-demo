"""DuckDB SQL transform: raw message-level parquet to materialised frames/trades/stats."""

from __future__ import annotations

from pathlib import Path

import sd_store as store


def _sq(value) -> str:
    """Single-quote and escape a value for safe inlining as a SQL literal."""
    return "'" + str(value).replace("'", "''") + "'"


FRAME_INSERT_COLUMNS = (
    ["symbol", "date", "scenario", "run", "frame_index", "time"]
    + store.FRAME_SCALAR_COLUMNS
    + store.BOOK_FRAME_COLUMNS
)

_RUN_SCALAR_EXPR = {
    "nbbo_bid": "nbbo_bid",
    "nbbo_ask": "nbbo_ask",
    "market_price": "market_price",
    "market_price_min": "NULL::DOUBLE",
    "market_price_max": "NULL::DOUBLE",
    "spread": "spread",
    "trade_count": "trade_count",
    "trade_volume": "trade_volume",
    "trade_vwap": "trade_vwap",
    "last_trade_price": "last_trade_price",
    "runs_reporting": "1",
    "day_open": "day_open",
    "day_high": "day_high",
    "day_low": "day_low",
    "day_close": "day_close",
    "day_volume": "day_volume",
}


def available_columns(path: Path) -> set[str]:
    cur = store.execute(f"DESCRIBE SELECT * FROM read_parquet({_sq(str(path))})")
    return {row[0] for row in cur.fetchall()}


def _snapshot_book_exprs(available: set[str]) -> list[str]:
    out = []
    for col in store.BOOK_FRAME_COLUMNS:
        if col in available:
            out.append(f"arg_max({col}, timestamp) AS {col}")
        else:
            out.append(f"NULL::DOUBLE AS {col}")
    return out


def insert_run_frames(symbol: str, date: str, scenario: str, run: str, path: Path, available: set[str]) -> None:
    nbbo_bid = "arg_max(bid_price_1, timestamp)" if "bid_price_1" in available else "NULL::DOUBLE"
    nbbo_ask = "arg_max(ask_price_1, timestamp)" if "ask_price_1" in available else "NULL::DOUBLE"
    snap_book = ",\n            ".join(_snapshot_book_exprs(available))
    joined_book = ",\n            ".join(f"s.{c} AS {c}" for c in store.BOOK_FRAME_COLUMNS)

    select_exprs = [_sq(symbol), _sq(date), _sq(scenario), _sq(run), "frame_index",
                    "strftime(sec, '%Y-%m-%dT%H:%M:%S')"]
    select_exprs += [_RUN_SCALAR_EXPR[c] for c in store.FRAME_SCALAR_COLUMNS]
    select_exprs += list(store.BOOK_FRAME_COLUMNS)

    sql = f"""
    INSERT INTO frames ({", ".join(FRAME_INSERT_COLUMNS)})
    WITH base AS (
        SELECT *, date_trunc('second', timestamp) AS sec
        FROM read_parquet({_sq(str(path))})
    ),
    snap AS (
        SELECT
            sec,
            {nbbo_bid} AS nbbo_bid,
            {nbbo_ask} AS nbbo_ask,
            {snap_book}
        FROM base
        GROUP BY sec
    ),
    tr AS (
        SELECT
            sec,
            count(*) AS trade_count,
            sum(size) AS trade_volume,
            sum(price * size) / nullif(sum(size), 0) AS trade_vwap,
            arg_max(price, timestamp) AS last_trade_price
        FROM base
        WHERE message_type = 2
        GROUP BY sec
    ),
    joined AS (
        SELECT
            s.sec AS sec,
            s.nbbo_bid AS nbbo_bid,
            s.nbbo_ask AS nbbo_ask,
            (s.nbbo_bid + s.nbbo_ask) / 2.0 AS market_price,
            (s.nbbo_ask - s.nbbo_bid) AS spread,
            COALESCE(t.trade_count, 0) AS trade_count,
            COALESCE(t.trade_volume, 0) AS trade_volume,
            COALESCE(t.trade_vwap, (s.nbbo_bid + s.nbbo_ask) / 2.0) AS trade_vwap,
            COALESCE(t.last_trade_price, (s.nbbo_bid + s.nbbo_ask) / 2.0) AS last_trade_price,
            {joined_book}
        FROM snap s
        LEFT JOIN tr t ON s.sec = t.sec
    ),
    ordered AS (
        SELECT
            *,
            (row_number() OVER (ORDER BY sec) - 1) AS frame_index,
            first_value(last_trade_price) OVER w AS day_open,
            max(last_trade_price) OVER w AS day_high,
            min(last_trade_price) OVER w AS day_low,
            last_trade_price AS day_close,
            sum(trade_volume) OVER w AS day_volume
        FROM joined
        WINDOW w AS (ORDER BY sec ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    )
    SELECT {", ".join(select_exprs)}
    FROM ordered
    ORDER BY frame_index
    """
    store.execute(sql)


def insert_run_trades(symbol: str, date: str, scenario: str, run: str, path: Path, available: set[str]) -> None:
    if "message_type" not in available:
        return
    side_expr = "CASE WHEN side = 1 THEN 'bid' ELSE 'ask' END" if "side" in available else "NULL"
    price_expr = "price" if "price" in available else "NULL::DOUBLE"
    size_expr = "size" if "size" in available else "NULL::DOUBLE"
    # order_id is a free-form string in the raw data (e.g. "MT-HF|9-mF-2"), so keep it as text.
    order_expr = "CAST(order_id AS VARCHAR)" if "order_id" in available else "NULL::VARCHAR"

    sql = f"""
    INSERT INTO trades (symbol, date, scenario, run, bucket, seq, time, side, price, size,
                        order_id, trade_count, trade_volume, trade_vwap)
    SELECT
        {_sq(symbol)}, {_sq(date)}, {_sq(scenario)}, {_sq(run)},
        strftime(date_trunc('second', timestamp), '%Y-%m-%dT%H:%M:%S') AS bucket,
        row_number() OVER (ORDER BY timestamp) AS seq,
        strftime(timestamp, '%Y-%m-%dT%H:%M:%S.%g') AS time,
        {side_expr} AS side,
        {price_expr} AS price,
        {size_expr} AS size,
        {order_expr} AS order_id,
        NULL::BIGINT AS trade_count,
        NULL::DOUBLE AS trade_volume,
        NULL::DOUBLE AS trade_vwap
    FROM read_parquet({_sq(str(path))})
    WHERE message_type = 2
    """
    store.execute(sql)


def run_microstructure(path: Path, available: set[str]) -> dict:
    have_msg = "message_type" in available
    have_notional = {"message_type", "side", "price", "size"}.issubset(available)

    add_expr = "count(*) FILTER (WHERE message_type = 1)" if have_msg else "0"
    cancel_expr = "count(*) FILTER (WHERE message_type IN (3, 4, 5))" if have_msg else "0"
    trade_expr = "count(*) FILTER (WHERE message_type = 2)" if have_msg else "0"
    buy_expr = (
        "COALESCE(sum(price * size) FILTER (WHERE message_type = 2 AND side = 1), 0)"
        if have_notional
        else "0"
    )
    sell_expr = (
        "COALESCE(sum(price * size) FILTER (WHERE message_type = 2 AND side <> 1), 0)"
        if have_notional
        else "0"
    )

    row = store.execute(
        f"""
        SELECT
            count(*) AS total_messages,
            {add_expr} AS add_events,
            {cancel_expr} AS cancel_events,
            {trade_expr} AS trade_events,
            {buy_expr} AS buy_notional,
            {sell_expr} AS sell_notional
        FROM read_parquet({_sq(str(path))})
        """
    ).fetchone()

    total_messages = float(row[0] or 0)
    add_events = float(row[1] or 0)
    cancel_events = float(row[2] or 0)
    trade_events = float(row[3] or 0)
    buy_notional = float(row[4] or 0)
    sell_notional = float(row[5] or 0)
    total_notional = buy_notional + sell_notional
    return {
        "total_messages": total_messages,
        "add_events": add_events,
        "cancel_events": cancel_events,
        "trade_events": trade_events,
        "cancellation_rate": (cancel_events / total_messages) if total_messages else 0.0,
        "cancel_to_add_rate": (cancel_events / add_events) if add_events else 0.0,
        "buy_notional": buy_notional,
        "sell_notional": sell_notional,
        "total_notional": total_notional,
        "notional_imbalance": ((buy_notional - sell_notional) / total_notional) if total_notional else 0.0,
    }


def insert_aggregate_frames(symbol: str, date: str, scenario: str) -> None:
    agg_book = []
    for col in store.BOOK_FRAME_COLUMNS:
        agg = "sum" if "_size_" in col else "avg"
        agg_book.append(f"{agg}({col}) AS {col}")
    agg_book_sql = ",\n            ".join(agg_book)

    select_exprs = [_sq(symbol), _sq(date), _sq(scenario), "'all'", "frame_index", "time_str"]
    select_exprs += list(store.FRAME_SCALAR_COLUMNS)
    select_exprs += list(store.BOOK_FRAME_COLUMNS)

    sql = f"""
    INSERT INTO frames ({", ".join(FRAME_INSERT_COLUMNS)})
    WITH agg AS (
        SELECT
            time AS time_str,
            avg(nbbo_bid) AS nbbo_bid,
            avg(nbbo_ask) AS nbbo_ask,
            avg(market_price) AS market_price,
            min(market_price) AS market_price_min,
            max(market_price) AS market_price_max,
            avg(spread) AS spread,
            sum(trade_count) AS trade_count,
            sum(trade_volume) AS trade_volume,
            avg(trade_vwap) AS trade_vwap,
            avg(last_trade_price) AS last_trade_price,
            count(*) AS runs_reporting,
            {agg_book_sql}
        FROM frames
        WHERE symbol = {_sq(symbol)} AND date = {_sq(date)}
          AND scenario = {_sq(scenario)} AND run <> 'all'
        GROUP BY time
    ),
    ordered AS (
        SELECT
            *,
            (row_number() OVER (ORDER BY time_str) - 1) AS frame_index,
            first_value(last_trade_price) OVER w AS day_open,
            max(last_trade_price) OVER w AS day_high,
            min(last_trade_price) OVER w AS day_low,
            last_trade_price AS day_close,
            sum(trade_volume) OVER w AS day_volume
        FROM agg
        WINDOW w AS (ORDER BY time_str ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    )
    SELECT {", ".join(select_exprs)}
    FROM ordered
    ORDER BY frame_index
    """
    store.execute(sql)


def insert_aggregate_trades(symbol: str, date: str, scenario: str) -> None:
    sql = f"""
    INSERT INTO trades (symbol, date, scenario, run, bucket, seq, time, side, price, size,
                        order_id, trade_count, trade_volume, trade_vwap)
    SELECT
        {_sq(symbol)}, {_sq(date)}, {_sq(scenario)}, 'all',
        bucket,
        row_number() OVER (ORDER BY bucket) AS seq,
        bucket AS time,
        NULL AS side,
        NULL::DOUBLE AS price,
        NULL::DOUBLE AS size,
        NULL::VARCHAR AS order_id,
        count(*) AS trade_count,
        sum(size) AS trade_volume,
        sum(price * size) / nullif(sum(size), 0) AS trade_vwap
    FROM trades
    WHERE symbol = {_sq(symbol)} AND date = {_sq(date)}
      AND scenario = {_sq(scenario)} AND run <> 'all'
    GROUP BY bucket
    """
    store.execute(sql)


def _frame_derived(symbol: str, date: str, scenario: str, run: str) -> dict:
    row = store.execute(
        """
        SELECT count(*), avg(spread), min(market_price), max(market_price)
        FROM frames WHERE symbol=? AND date=? AND scenario=? AND run=?
        """,
        [symbol, date, scenario, run],
    ).fetchone()
    return {
        "frame_count": int(row[0] or 0),
        "avg_spread": float(row[1] or 0.0),
        "min_market_price": float(row[2] or 0.0),
        "max_market_price": float(row[3] or 0.0),
    }


def _trade_derived(symbol: str, date: str, scenario: str, run: str) -> dict:
    row = store.execute(
        """
        SELECT count(*), COALESCE(sum(COALESCE(size, trade_volume)), 0)
        FROM trades WHERE symbol=? AND date=? AND scenario=? AND run=?
        """,
        [symbol, date, scenario, run],
    ).fetchone()
    return {"trade_rows": int(row[0] or 0), "total_trade_volume": float(row[1] or 0.0)}


_RUN_STATS_COLUMNS = [
    "symbol", "date", "scenario", "run", "mode", "n_runs_available", "runs_included",
    "frame_count", "trade_rows", "total_trade_volume", "avg_spread", "min_market_price",
    "max_market_price", "total_messages", "add_events", "cancel_events", "trade_events",
    "cancellation_rate", "cancel_to_add_rate", "buy_notional", "sell_notional",
    "total_notional", "notional_imbalance",
]


def _insert_run_stats(values: dict) -> None:
    placeholders = ", ".join(["?"] * len(_RUN_STATS_COLUMNS))
    store.execute(
        f"INSERT OR REPLACE INTO run_stats ({', '.join(_RUN_STATS_COLUMNS)}) VALUES ({placeholders})",
        [values[c] for c in _RUN_STATS_COLUMNS],
    )


def _avg_micros(micros: list[dict]) -> dict:
    keys = [
        "total_messages", "add_events", "cancel_events", "trade_events",
        "cancellation_rate", "cancel_to_add_rate", "buy_notional", "sell_notional",
        "total_notional", "notional_imbalance",
    ]
    if not micros:
        return {k: 0.0 for k in keys}
    return {k: float(sum(m.get(k, 0.0) for m in micros) / len(micros)) for k in keys}


def delete_scenario(symbol: str, date: str, scenario: str) -> None:
    for table in ("frames", "trades", "run_stats"):
        store.execute(
            f"DELETE FROM {table} WHERE symbol=? AND date=? AND scenario=?",
            [symbol, date, scenario],
        )


def build_scenario(symbol: str, date: str, scenario: str, run_files: dict[int, Path], n_runs: int) -> dict:
    """Materialise every run plus the cross-run aggregate for one scenario."""
    delete_scenario(symbol, date, scenario)

    first_path = run_files[sorted(run_files)[0]]
    available = available_columns(first_path)

    micros: list[dict] = []
    for run_index in sorted(run_files):
        run = str(run_index)
        path = run_files[run_index]
        insert_run_frames(symbol, date, scenario, run, path, available)
        insert_run_trades(symbol, date, scenario, run, path, available)
        micro = run_microstructure(path, available)
        micros.append(micro)

        frame_d = _frame_derived(symbol, date, scenario, run)
        trade_d = _trade_derived(symbol, date, scenario, run)
        _insert_run_stats(
            {
                "symbol": symbol, "date": date, "scenario": scenario, "run": run,
                "mode": "single", "n_runs_available": n_runs, "runs_included": 1,
                **frame_d, **trade_d, **micro,
            }
        )

    insert_aggregate_frames(symbol, date, scenario)
    insert_aggregate_trades(symbol, date, scenario)
    all_frame_d = _frame_derived(symbol, date, scenario, "all")
    all_trade_d = _trade_derived(symbol, date, scenario, "all")
    _insert_run_stats(
        {
            "symbol": symbol, "date": date, "scenario": scenario, "run": "all",
            "mode": "aggregate", "n_runs_available": n_runs, "runs_included": n_runs,
            **all_frame_d, **all_trade_d, **_avg_micros(micros),
        }
    )

    return {
        "runs": len(run_files),
        "all_frames": all_frame_d["frame_count"],
        "all_trade_rows": all_trade_d["trade_rows"],
    }
