# Simudyne Market Stream

FastAPI backend that downloads cached Simudyne Pulse order-book simulations,
materialises them into DuckDB, and serves an OpenBB Workspace app: a streaming
order-book / tick-chart / trade-tape player plus a scenario-vs-run stats table.

## Run it

```bash
cp .env.example .env          # set SIMUDYNE_API_KEY
docker compose up --build -d
```

Served at `http://localhost:6770`. On startup it downloads and materialises every
allowed scenario for every allowed symbol and date, and every run; it blocks until complete.

Local dev without Docker (needs the Simudyne SDK):

```bash
pip install -r requirements.txt openbb-platform-api
pip install "git+https://github.com/simudyne/pulse-sdk.git@<ref>"
SIMUDYNE_API_KEY=... SIMUDYNE_DATA_DIR=./.simudyne_data \
  uvicorn simudyne_duck_app:app --host 0.0.0.0 --port 6770
```

## OpenBB Workspace

Add `http://localhost:6770` as a custom backend. It serves `/apps.json` and
`/widgets.json`, exposing one app with two tabs:

- **Market Stream** — the iframe player (`simudyne_market_stream_iframe`) and the
  stats table (`simudyne_stats_markdown`). Symbol / date / scenario / run are
  dependent dropdowns backed by `/simudyne_param_options`.
- **About** — a markdown widget describing Simudyne Pulse.

The iframe honours the `theme=light|dark` param Workspace passes.

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/simudyne_stream` | iframe player (HTML; assets under `/simudyne_assets/`) |
| WS | `/ws/simudyne_stream` | frame-by-frame playback at the selected cadence |
| GET | `/simudyne_stream_data` | full payload (meta + stats + frames + trades) as JSON |
| GET | `/simudyne_stats` | selected run KPIs vs the scenario (`all`) average |
| GET | `/simudyne_param_options` | dependent-dropdown options (symbol→date→scenario→run) |
| GET | `/simudyne_config` | player defaults (symbol, date, scenarios) |
| GET | `/simudyne_about` | About markdown (with the plot inlined) |
| GET | `/simudyne_cache_status` | warmup state, progress, event log |
| POST | `/simudyne_cache_warm` | start a warmup (`?force_refresh=true` to rebuild) |
| POST | `/simudyne_cache_cancel` | cancel a running warmup |
| GET | `/apps.json`, `/widgets.json` | OpenBB Workspace definitions |
| GET | `/health` | liveness |

Query params for the stream/stats endpoints: `symbol`, `date`, `scenario`, `run`
(a run index or `all`), plus `playback_ms` for the websocket.

## Environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `SIMUDYNE_API_KEY` | — | **required** |
| `SIMUDYNE_SYMBOL` / `SIMUDYNE_DATE` | — | default selection for the player |
| `SIMUDYNE_ALLOWED_SYMBOLS` | — | comma-separated allow-list (empty = any) |
| `SIMUDYNE_ALLOWED_DATES` | — | comma-separated allow-list (empty = any) |
| `SIMUDYNE_ALLOWED_SCENARIOS` | `flash_crash,buy_panic,normal,trending_up` | comma-separated allow-list |
| `SIMUDYNE_DATA_DIR` | `/app/.simudyne_data` | DuckDB db + raw parquet location |
| `SIMUDYNE_STARTUP_FORCE_REFRESH` | `1` | rebuild the store on startup even if data exists |
| `SIMUDYNE_STARTUP_BLOCKING_WARMUP` | `1` | block startup until warmup completes |
| `SIMUDYNE_KEEP_RAW_PARQUET` | `1` | keep raw parquet (`0` = delete after materialise) |
| `SIMUDYNE_WARMUP_WORKERS` | `8` | parallel scenario downloads |
| `SIMUDYNE_DUCKDB_THREADS` | `4` | DuckDB query threads |
| `SIMUDYNE_API_TIMEOUT_SEC` / `_RETRIES` / `_RETRY_BACKOFF_SEC` | `120` / `2` / `2.0` | API call resilience |

> Schema changes (e.g. a `trades` column type) only apply on a force-refresh,
> which drops and recreates the tables. A plain restart against an existing
> `stream.duckdb` volume keeps the old schema — rebuild with
> `SIMUDYNE_STARTUP_FORCE_REFRESH=1` (the default) or delete the volume.

## Data model

Three DuckDB tables, keyed by `(symbol, date, scenario, run)` where `run` is a run
index (`"0"`, `"1"`, …) or `"all"` (the cross-run aggregate):

- **`frames`** — one row per second: NBBO, mid/spread, per-second trade
  count/volume/VWAP, cumulative day OHLC + volume, and 10 book levels
  (missing levels are `NULL`).
- **`trades`** — per-run individual trades (`side`, `price`, `size`, `order_id`
  as a string, millisecond `time`); for the `all` view, per-second aggregates
  (`trade_count`, `trade_volume`, `trade_vwap`). Bucketed by second to line up
  with `frames.time`.
- **`run_stats`** — one KPI row per selection; `/simudyne_stats` compares a run
  against its scenario's `all` row.

## Layout

```
simudyne_duck_app.py   FastAPI app: endpoints, websocket, lifespan warmup
sd_config.py           env-driven config + allow-list validators
sd_client.py           Simudyne SDK wrapper: list_cached, download runs -> parquet
sd_transform.py        DuckDB SQL: parquet -> frames / trades / run_stats
sd_store.py            DuckDB connection, schema, manifest, read queries
sd_ingest.py           warmup orchestration (parallel download, serial materialise)
sd_status.py           warmup status, event log, cancel event
assets/                iframe player: stream.html, stream.css, stream.js
simudyne_apps.json     OpenBB app template (tabs + parameter groups)
simudyne_widgets.json  OpenBB widget definitions
```

Data directory (named volume `/app/.simudyne_data`):

```
stream.duckdb                                       frames / trades / run_stats / kv
raw/<symbol>/<date>/<scenario>/run_NNNN.parquet     raw LOB, queried directly by DuckDB
```
