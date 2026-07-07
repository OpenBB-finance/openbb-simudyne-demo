"""Warmup orchestration: discover scenarios, download in parallel, materialise into DuckDB."""

from __future__ import annotations

import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

import sd_client as client
import sd_config as cfg
import sd_store as store
import sd_transform as transform
from sd_status import (
    WARMUP_CANCEL_EVENT,
    WARMUP_LOCK,
    WARMUP_STATUS,
    WarmupCancelled,
    now_iso,
    record_event,
)

WARMUP_THREAD: threading.Thread | None = None


def warmup_plan(cancel_event: threading.Event | None = None) -> list[tuple[str, str, str]]:
    plan: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    scenarios_by_symbol_date: dict[tuple[str, str], set[str]] = {}

    def _consider(rows: list[dict], source: str) -> None:
        for row in rows:
            symbol = str(row.get("symbol") or "").strip()
            date = str(row.get("date") or "").strip()
            scenario = str(row.get("scenario") or "").strip()
            if cfg.SIMUDYNE_ALLOWED_SYMBOLS_SET and symbol not in cfg.SIMUDYNE_ALLOWED_SYMBOLS_SET:
                continue
            if cfg.SIMUDYNE_ALLOWED_DATES_SET and date not in cfg.SIMUDYNE_ALLOWED_DATES_SET:
                continue
            if cfg.SIMUDYNE_ALLOWED_SCENARIOS_SET and scenario not in cfg.SIMUDYNE_ALLOWED_SCENARIOS_SET:
                continue
            if not symbol or not date or not scenario:
                continue
            key = (symbol, date, scenario)
            if key in seen:
                continue
            seen.add(key)
            scenarios_by_symbol_date.setdefault((symbol, date), set()).add(scenario)
            plan.append(key)
            record_event(
                "discover_scenario_completed",
                symbol=symbol, date=date, scenario=scenario, source=source,
            )

    if cancel_event is not None and cancel_event.is_set():
        raise WarmupCancelled("Warmup cancelled")

    _consider(client.list_cached_simulations(cancel_event=cancel_event), "global_list_cached")

    if not plan:
        fallback_dates = list(cfg.SIMUDYNE_ALLOWED_DATES) if cfg.SIMUDYNE_ALLOWED_DATES else [cfg.DEFAULT_SIMUDYNE_DATE]
        for date in fallback_dates:
            if cancel_event is not None and cancel_event.is_set():
                raise WarmupCancelled("Warmup cancelled")
            _consider(client.list_cached_simulations(date=date, cancel_event=cancel_event), "fallback_by_date")

    if cfg.SIMUDYNE_ALLOWED_SCENARIOS_SET:
        required = set(cfg.SIMUDYNE_ALLOWED_SCENARIOS)
        missing: list[str] = []
        for (symbol, date), found in sorted(scenarios_by_symbol_date.items()):
            gap = sorted(required - found)
            if gap:
                missing.append(f"{symbol}:{date} missing {','.join(gap)}")
        if missing:
            raise RuntimeError("Scenario completeness check failed for required scenarios: " + "; ".join(missing))

    return plan


def _run_warmup(
    force_refresh: bool = False,
    cancel_event: threading.Event | None = None,
    raise_on_error: bool = False,
) -> None:
    cancel_event = cancel_event or threading.Event()

    def _check_cancel() -> None:
        if cancel_event.is_set():
            raise WarmupCancelled("Warmup cancelled")

    global WARMUP_THREAD
    with WARMUP_LOCK:
        WARMUP_STATUS.update(
            {
                "state": "running",
                "started_at": now_iso(),
                "finished_at": None,
                "completed": 0,
                "total": 0,
                "current": "discovering cached scenarios",
                "current_started_at": time.time(),
                "error": None,
            }
        )
    record_event("warmup_started", force_refresh=force_refresh)

    try:
        _check_cancel()
        plan = warmup_plan(cancel_event=cancel_event)
        _check_cancel()

        total = len(plan)
        if total == 0:
            allowed = ", ".join(cfg.SIMUDYNE_ALLOWED_DATES) or "<any>"
            raise RuntimeError(f"No cached baseline simulations found for allowed dates: {allowed}")
        with WARMUP_LOCK:
            WARMUP_STATUS["total"] = total
        record_event("warmup_plan_ready", steps=total)

        if force_refresh:
            store.reset_all()
            record_event("store_reset")

        workers = min(cfg.WARMUP_MAX_WORKERS, total)
        record_event("warmup_downloads_starting", workers=workers, total=total)

        runs_by_scenario: dict[str, int] = {}
        first_error: Exception | None = None
        cancelled = False
        executor = ThreadPoolExecutor(max_workers=workers)
        try:
            futures: dict = {}
            for symbol, date, scenario in plan:
                _check_cancel()
                fut = executor.submit(
                    client.download_scenario_parquet, symbol, date, scenario, cancel_event
                )
                futures[fut] = (symbol, date, scenario)

            pending = set(futures)
            while pending:
                if cancel_event.is_set():
                    cancelled = True
                    for fut in pending:
                        fut.cancel()
                    raise WarmupCancelled("Warmup cancelled")

                done, pending = wait(pending, timeout=0.2, return_when=FIRST_COMPLETED)
                for fut in done:
                    symbol, date, scenario = futures[fut]
                    exc = fut.exception()
                    if exc is not None:
                        record_event(
                            "warmup_step_failed", level="error",
                            symbol=symbol, date=date, scenario=scenario, error=str(exc),
                        )
                        if first_error is None:
                            first_error = exc
                        continue

                    _, n_runs, run_files = fut.result()
                    with WARMUP_LOCK:
                        WARMUP_STATUS["current"] = f"materialising {symbol} {date} {scenario}"
                        WARMUP_STATUS["current_started_at"] = time.time()
                    step_start = time.time()
                    summary = transform.build_scenario(symbol, date, scenario, run_files, n_runs)
                    runs_by_scenario[f"{symbol}:{date}:{scenario}"] = n_runs

                    if not cfg.KEEP_RAW_PARQUET:
                        for path in run_files.values():
                            try:
                                path.unlink()
                            except OSError:
                                pass

                    with WARMUP_LOCK:
                        WARMUP_STATUS["completed"] += 1
                        completed = WARMUP_STATUS["completed"]
                    record_event(
                        "warmup_step_completed",
                        symbol=symbol, date=date, scenario=scenario,
                        elapsed_sec=round(time.time() - step_start, 3),
                        completed=completed, total=total, **summary,
                    )
        finally:
            executor.shutdown(wait=not cancelled, cancel_futures=cancelled)

        if first_error is not None:
            raise first_error

        store.write_manifest(plan, runs_by_scenario)
        store.checkpoint()
        ready, checked, integrity_total, reason = store.is_prebuilt_complete()
        if not ready:
            raise RuntimeError(
                f"Ingestion integrity check failed after build: reason={reason}, "
                f"checked={checked}, total={integrity_total}"
            )
        record_event("warmup_integrity_verified", checked=checked, total=integrity_total)

        with WARMUP_LOCK:
            WARMUP_STATUS.update(
                {"state": "done", "finished_at": now_iso(), "current": None, "current_started_at": None}
            )
        record_event("warmup_completed", completed=WARMUP_STATUS["completed"], total=total)
    except WarmupCancelled:
        with WARMUP_LOCK:
            WARMUP_STATUS.update(
                {"state": "cancelled", "error": None, "finished_at": now_iso(),
                 "current": None, "current_started_at": None}
            )
        record_event("warmup_cancelled", level="warning")
        return
    except Exception as exc:
        with WARMUP_LOCK:
            WARMUP_STATUS.update(
                {"state": "error", "error": str(exc), "finished_at": now_iso(),
                 "current": None, "current_started_at": None}
            )
        record_event("warmup_failed", level="error", error=str(exc))
        if raise_on_error:
            raise
        return
    finally:
        with WARMUP_LOCK:
            if WARMUP_THREAD is threading.current_thread():
                WARMUP_THREAD = None


def start_warmup_thread(force_refresh: bool, source: str) -> bool:
    global WARMUP_THREAD
    with WARMUP_LOCK:
        if WARMUP_THREAD is not None and WARMUP_THREAD.is_alive():
            return False
        WARMUP_CANCEL_EVENT.clear()
        thread = threading.Thread(
            target=_run_warmup,
            kwargs={"force_refresh": force_refresh, "cancel_event": WARMUP_CANCEL_EVENT},
            daemon=True,
            name="simudyne-warmup",
        )
        WARMUP_THREAD = thread
        thread.start()
    record_event("warmup_thread_started", source=source, force_refresh=force_refresh)
    return True


def request_warmup_cancel(source: str) -> bool:
    with WARMUP_LOCK:
        running = WARMUP_STATUS.get("state") == "running"
        if running:
            WARMUP_STATUS["state"] = "cancelling"
    if not running:
        return False
    WARMUP_CANCEL_EVENT.set()
    record_event("warmup_cancel_requested", source=source)
    return True


def run_warmup_blocking(force_refresh: bool = False) -> None:
    _run_warmup(force_refresh=force_refresh, cancel_event=WARMUP_CANCEL_EVENT, raise_on_error=True)
