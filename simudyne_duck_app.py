"""FastAPI app: DuckDB-backed Simudyne market-stream service."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import asyncio
import base64
import functools
import json
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

import sd_config as cfg
import sd_ingest as ingest
import sd_store as store
from sd_status import WARMUP_LOCK, WARMUP_STATUS, now_iso, record_event

app = FastAPI(title="Simudyne Market Stream (DuckDB)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://pro.openbb.co", "https://pro.openbb.dev", "http://localhost:3000", "http://127.0.0.1:6770"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# Serve the iframe player's static assets (HTML/CSS/JS) from ./assets.
app.mount("/simudyne_assets", StaticFiles(directory=str(cfg.ASSETS_DIR)), name="simudyne_assets")


@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "ok"}


@app.get("/apps.json", include_in_schema=False)
async def apps_json():
    return json.loads((cfg.ROOT / "simudyne_apps.json").read_text(encoding="utf-8"))


@app.get("/widgets.json", include_in_schema=False)
async def widgets_json():
    return json.loads((cfg.ROOT / "simudyne_widgets.json").read_text(encoding="utf-8"))


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)


@functools.lru_cache(maxsize=1)
def _about_markdown() -> str:
    """Load the About description, inlining the plot as a data URI so it renders
    inside the OpenBB markdown widget regardless of how the backend is hosted."""
    md = (cfg.ROOT / "openbb_widget_description.md").read_text(encoding="utf-8")
    img = cfg.ROOT / "plot.png"
    if img.exists():
        data_uri = "data:image/png;base64," + base64.b64encode(img.read_bytes()).decode("ascii")
        md = md.replace("(plot.png)", f"({data_uri})")
    return md


@app.get("/simudyne_about", include_in_schema=False)
async def simudyne_about() -> str:
    return _about_markdown()


def _require_payload(symbol: str, date: str, scenario: str, run: str) -> dict:
    payload = store.get_payload(symbol, date, scenario, run)
    if payload is None:
        raise RuntimeError(
            f"No materialised data for symbol={symbol}, date={date}, scenario={scenario}, run={run}. "
            "Startup warmup must finish before this selection is available."
        )
    return payload


@app.get("/simudyne_stream_data", include_in_schema=False)
async def simudyne_stream_data(
    symbol: str = Query(cfg.DEFAULT_SIMUDYNE_SYMBOL),
    date: str = Query(cfg.DEFAULT_SIMUDYNE_DATE),
    scenario: str = Query("flash_crash"),
    run: str = Query("0"),
):
    try:
        symbol = cfg.ensure_allowed_symbol(symbol)
        date = cfg.ensure_allowed_date(date)
        scenario = cfg.ensure_allowed_scenario(scenario)
        payload = _require_payload(symbol, date, scenario, run)
        frames = store.get_frames(symbol, date, scenario, run)
        trade_rows = store.get_trade_rows(symbol, date, scenario, run)
        return {**payload, "frames": frames, "trade_rows": trade_rows}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/simudyne_stats", include_in_schema=False)
async def simudyne_stats(
    symbol: str = Query(cfg.DEFAULT_SIMUDYNE_SYMBOL),
    date: str = Query(cfg.DEFAULT_SIMUDYNE_DATE),
    scenario: str = Query("flash_crash"),
    run: str = Query("0"),
):
    with WARMUP_LOCK:
        warmup_state = str(WARMUP_STATUS.get("state") or "idle")
        warmup_error = WARMUP_STATUS.get("error")
    if warmup_state in {"running", "cancelling"}:
        raise HTTPException(
            status_code=503, detail="Data ingestion in progress; stats will be available after warmup completes"
        )
    if warmup_state == "error":
        detail = f"Data ingestion failed: {warmup_error}" if warmup_error else "Data ingestion failed"
        raise HTTPException(status_code=503, detail=detail)

    try:
        symbol = cfg.ensure_allowed_symbol(symbol)
        date = cfg.ensure_allowed_date(date)
        scenario = cfg.ensure_allowed_scenario(scenario)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    rows = store.get_stats_rows(symbol, date, scenario, run)
    if rows is None:
        raise HTTPException(
            status_code=503,
            detail="Stats not materialised yet for this selection. Warmup must finish all runs first.",
        )
    return rows


@app.get("/simudyne_cache_status", include_in_schema=False)
async def simudyne_cache_status(tail: int = Query(50, ge=1, le=500)):
    with WARMUP_LOCK:
        payload = {k: v for k, v in WARMUP_STATUS.items() if k != "events"}
        payload["events"] = WARMUP_STATUS["events"][-tail:]
    payload["frames_rows"] = int(store.execute("SELECT count(*) FROM frames").fetchone()[0])
    payload["scenarios_materialised"] = int(store.execute("SELECT count(*) FROM run_stats WHERE run='all'").fetchone()[0])
    payload["data_dir"] = str(cfg.DATA_DIR)
    payload["allowed_dates"] = list(cfg.SIMUDYNE_ALLOWED_DATES)
    payload["allowed_symbols"] = list(cfg.SIMUDYNE_ALLOWED_SYMBOLS)
    payload["allowed_scenarios"] = list(cfg.SIMUDYNE_ALLOWED_SCENARIOS)
    total = int(payload.get("total") or 0)
    completed = int(payload.get("completed") or 0)
    payload["progress_pct"] = round((completed / total) * 100, 2) if total else 0.0
    started = payload.get("current_started_at")
    payload["current_elapsed_sec"] = round(time.time() - started, 3) if started else None
    return payload


@app.get("/simudyne_param_options", include_in_schema=False)
async def simudyne_param_options(
    symbol: str = Query(""),
    date: str = Query(""),
    scenario: str = Query(""),
):
    symbol, date, scenario = symbol.strip(), date.strip(), scenario.strip()
    if scenario:
        values = store.distinct_runs(symbol, date, scenario) or ["0", "all"]
    elif date:
        values = store.distinct_scenarios(symbol, date) or (
            list(cfg.SIMUDYNE_ALLOWED_SCENARIOS) or list(cfg.DEFAULT_SCENARIO_OPTIONS)
        )
    elif symbol:
        values = store.distinct_dates(symbol) or list(cfg.SIMUDYNE_ALLOWED_DATES)
    else:
        values = store.distinct_symbols() or (
            [cfg.DEFAULT_SIMUDYNE_SYMBOL] if cfg.DEFAULT_SIMUDYNE_SYMBOL else []
        )
    return [{"label": v, "value": v} for v in values]


@app.post("/simudyne_cache_warm", include_in_schema=False)
async def simudyne_cache_warm(force_refresh: bool = Query(False)):
    started = ingest.start_warmup_thread(force_refresh=force_refresh, source="manual")
    if not started:
        return {"detail": "warmup already running"}
    return {"detail": "warmup started", "force_refresh": force_refresh}


@app.post("/simudyne_cache_cancel", include_in_schema=False)
async def simudyne_cache_cancel():
    cancelled = ingest.request_warmup_cancel(source="manual")
    if not cancelled:
        return {"detail": "no warmup running"}
    return {"detail": "warmup cancellation requested"}


@asynccontextmanager
async def lifespan(_: FastAPI):
    ready, checked, total, reason = store.is_prebuilt_complete()
    if not cfg.STARTUP_FORCE_REFRESH and ready:
        with WARMUP_LOCK:
            WARMUP_STATUS.update(
                {
                    "state": "done",
                    "started_at": now_iso(),
                    "finished_at": now_iso(),
                    "completed": total,
                    "total": total,
                    "current": None,
                    "current_started_at": None,
                    "error": None,
                }
            )
        record_event("startup_data_reused", checked=checked, total=total)
        try:
            yield
        finally:
            ingest.request_warmup_cancel(source="shutdown")
            store.close()
        return

    if cfg.STARTUP_FORCE_REFRESH:
        record_event("startup_refresh_forced", reason="force_refresh_enabled")
    else:
        record_event("startup_data_not_ready", checked=checked, total=total, reason=reason)

    if cfg.STARTUP_BLOCKING_WARMUP:
        record_event("startup_warmup_mode", mode="blocking")
        await asyncio.to_thread(ingest.run_warmup_blocking, cfg.STARTUP_FORCE_REFRESH)
    else:
        record_event("startup_warmup_mode", mode="background")
        ingest.start_warmup_thread(force_refresh=cfg.STARTUP_FORCE_REFRESH, source="startup")
    try:
        yield
    finally:
        ingest.request_warmup_cancel(source="shutdown")
        store.close()


app.router.lifespan_context = lifespan


@app.websocket("/ws/simudyne_stream")
async def ws_simudyne_stream(
    websocket: WebSocket,
    symbol: str = Query(cfg.DEFAULT_SIMUDYNE_SYMBOL),
    date: str = Query(cfg.DEFAULT_SIMUDYNE_DATE),
    scenario: str = Query("flash_crash"),
    run: str = Query("0"),
    playback_ms: int = Query(120),
) -> None:
    await websocket.accept()
    try:
        symbol = cfg.ensure_allowed_symbol(symbol)
        date = cfg.ensure_allowed_date(date)
        scenario = cfg.ensure_allowed_scenario(scenario)
        payload = _require_payload(symbol, date, scenario, run)
        nframes = store.get_nframes(symbol, date, scenario, run)
        if not nframes:
            raise RuntimeError("payload not found in store")
    except (ValueError, RuntimeError) as exc:
        await websocket.send_json({"type": "error", "message": str(exc)})
        await websocket.close()
        return

    await websocket.send_json(
        {"type": "init", "meta": payload["meta"], "stats": payload["stats"], "total_frames": nframes}
    )

    frame_index = 0
    delay = max(playback_ms, 20) / 1000.0
    frame_step = 1
    paused = False
    dirty = True
    recv_task: asyncio.Task = asyncio.create_task(websocket.receive_json())

    try:
        while True:
            if recv_task.done():
                try:
                    msg = recv_task.result()
                    action = msg.get("action", "") if isinstance(msg, dict) else ""
                    if action == "pause":
                        paused = True
                    elif action == "resume":
                        paused = False
                    elif action == "restart":
                        frame_index = 0
                        dirty = True
                    elif action == "seek":
                        target = int(msg.get("index", frame_index))
                        frame_index = max(0, min(target, nframes - 1))
                        dirty = True
                    elif action == "set_speed":
                        playback_ms = max(20, int(msg.get("playback_ms", playback_ms)))
                        delay = playback_ms / 1000.0
                        frame_step = max(1, int(msg.get("frame_step", frame_step)))
                except Exception:
                    break
                recv_task = asyncio.create_task(websocket.receive_json())

            if (not paused) or dirty:
                frame = store.get_frame(symbol, date, scenario, run, frame_index)
                if frame is None:
                    break
                trades_this_frame = store.get_trades_for_second(symbol, date, scenario, run, frame["time"])
                await websocket.send_json(
                    {"type": "frame", "i": frame_index, "n": nframes, "f": frame, "t": trades_this_frame}
                )
                dirty = False
                if not paused:
                    frame_index = (frame_index + frame_step) % nframes

            await asyncio.sleep(0.05 if paused else delay)
    except WebSocketDisconnect:
        pass
    finally:
        recv_task.cancel()
        try:
            await recv_task
        except Exception:
            pass


@app.get("/simudyne_config", include_in_schema=False)
async def simudyne_config():
    """Defaults the iframe player reads at boot (no server-side HTML injection)."""
    return {
        "default_symbol": cfg.DEFAULT_SIMUDYNE_SYMBOL,
        "default_date": cfg.DEFAULT_SIMUDYNE_DATE,
        "scenarios": list(cfg.SIMUDYNE_ALLOWED_SCENARIOS) or list(cfg.DEFAULT_SCENARIO_OPTIONS),
    }


@app.get("/simudyne_stream", include_in_schema=False)
async def simudyne_stream():
    return FileResponse(cfg.ASSETS_DIR / "stream.html", media_type="text/html")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=6770)
