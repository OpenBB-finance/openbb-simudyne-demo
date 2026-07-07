"""Simudyne Pulse SDK wrapper: list cached sims, resolve sim_ids, download scenario parquet."""

from __future__ import annotations

import io
import os
import queue
import threading
import time
import zipfile
from pathlib import Path

import sd_config as cfg
from sd_status import WARMUP_CANCEL_EVENT, WarmupCancelled, record_event


def load_api_key() -> str:
    env_key = os.getenv("SIMUDYNE_API_KEY")
    if env_key:
        return env_key
    raise RuntimeError("SIMUDYNE_API_KEY not found. Set the environment variable.")


def load_client():
    try:
        from simudyne import PulseABM
    except ImportError as exc:
        raise RuntimeError("Missing dependency: simudyne") from exc
    return PulseABM(api_key=load_api_key())


def _run_blocking_call(
    label: str,
    fn,
    *args,
    timeout_sec: float | None = None,
    cancel_event: threading.Event | None = None,
    **kwargs,
):
    result_queue: queue.Queue = queue.Queue(maxsize=1)

    def _target() -> None:
        try:
            result_queue.put((True, fn(*args, **kwargs)))
        except Exception as exc:
            result_queue.put((False, exc))

    thread = threading.Thread(target=_target, daemon=True, name=f"simudyne-{label}")
    thread.start()

    deadline = None if timeout_sec is None or timeout_sec <= 0 else (time.time() + timeout_sec)
    while True:
        if cancel_event is not None and cancel_event.is_set():
            raise WarmupCancelled(f"{label} cancelled")
        try:
            ok, value = result_queue.get(timeout=0.2)
        except queue.Empty:
            if deadline is not None and time.time() >= deadline:
                raise RuntimeError(f"{label} timed out after {timeout_sec:.1f}s")
            continue
        if ok:
            return value
        raise value


def run_blocking_call_with_retries(
    label: str,
    fn,
    *args,
    timeout_sec: float | None = None,
    cancel_event: threading.Event | None = None,
    **kwargs,
):
    last_exc: Exception | None = None
    attempts = cfg.SIMUDYNE_API_RETRIES + 1
    for attempt in range(1, attempts + 1):
        if cancel_event is not None and cancel_event.is_set():
            raise WarmupCancelled(f"{label} cancelled")
        try:
            return _run_blocking_call(
                label, fn, *args, timeout_sec=timeout_sec, cancel_event=cancel_event, **kwargs
            )
        except WarmupCancelled:
            raise
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            record_event(
                "blocking_call_retry",
                level="warning",
                call=label,
                attempt=attempt,
                max_attempts=attempts,
                error=str(exc),
            )
            if cfg.SIMUDYNE_API_RETRY_BACKOFF_SEC > 0:
                end_sleep = time.time() + cfg.SIMUDYNE_API_RETRY_BACKOFF_SEC
                while time.time() < end_sleep:
                    if cancel_event is not None and cancel_event.is_set():
                        raise WarmupCancelled(f"{label} cancelled")
                    time.sleep(0.1)

    raise RuntimeError(f"{label} failed after {attempts} attempts: {last_exc}")


def list_cached_simulations(
    symbol: str | None = None,
    date: str | None = None,
    scenario: str | None = None,
    cancel_event: threading.Event | None = None,
) -> list[dict]:
    if date:
        cfg.ensure_allowed_date(date)

    client = load_client()
    kwargs: dict[str, str] = {}
    if symbol:
        kwargs["symbol"] = symbol
    if date:
        kwargs["date"] = date
    if scenario:
        kwargs["scenario"] = scenario

    cached = run_blocking_call_with_retries(
        "list_cached",
        client.simulation.list_cached,
        timeout_sec=cfg.SIMUDYNE_API_TIMEOUT_SEC,
        cancel_event=cancel_event,
        **kwargs,
    )
    if isinstance(cached, dict):
        simulations = cached.get("simulations") or []
    elif isinstance(cached, list):
        simulations = cached
    else:
        simulations = []
    return [row for row in simulations if isinstance(row, dict)]


def cached_config(
    scenario: str,
    symbol: str,
    date: str,
    cancel_event: threading.Event | None = None,
) -> dict:
    scenario = cfg.ensure_allowed_scenario(scenario)
    cfg.ensure_allowed_date(date)
    simulations = list_cached_simulations(
        symbol=symbol, date=date, scenario=scenario, cancel_event=cancel_event
    )
    if not simulations:
        raise ValueError(f"No cached simulations found for symbol={symbol}, date={date}, scenario={scenario}")
    return simulations[0]


def sim_ids_for(example_sim_id: str, n_runs: int) -> list[str]:
    return [f"{example_sim_id[:-4]}{index:04d}" for index in range(n_runs)]


def member_run_index(member_name: str) -> int:
    folder = member_name.split("/", 1)[0]
    return int(folder.rsplit("_", 1)[1])


def download_scenario_parquet(
    symbol: str,
    date: str,
    scenario: str,
    cancel_event: threading.Event | None = None,
) -> tuple[dict, int, dict[int, Path]]:
    """Download a scenario's runs in one bulk call and persist each run's parquet to disk."""
    symbol = cfg.ensure_allowed_symbol(symbol)
    date = cfg.ensure_allowed_date(date)
    scenario = cfg.ensure_allowed_scenario(scenario)

    cached = cached_config(scenario, symbol=symbol, date=date, cancel_event=cancel_event)
    n_runs = int(cached["n_runs"])
    sim_ids = sim_ids_for(cached["example_sim_id"], n_runs)

    record_event(
        "bulk_download_started", symbol=symbol, date=date, scenario=scenario, sim_count=len(sim_ids)
    )
    started = time.time()
    client = load_client()
    zip_bytes = run_blocking_call_with_retries(
        "get_bulk_data",
        client.simulation.get_bulk_data,
        sim_ids=sim_ids,
        include_sim_data=True,
        include_mid_price=False,
        timeout_sec=cfg.SIMUDYNE_API_TIMEOUT_SEC,
        cancel_event=cancel_event,
    )
    record_event(
        "bulk_download_completed",
        symbol=symbol,
        date=date,
        scenario=scenario,
        bytes=len(zip_bytes),
        elapsed_sec=round(time.time() - started, 3),
    )

    dest_dir = cfg.RAW_DIR / symbol / date / scenario
    dest_dir.mkdir(parents=True, exist_ok=True)

    run_files: dict[int, Path] = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        members = [name for name in archive.namelist() if name.endswith("sim_data.parquet")]
        for member_name in sorted(members):
            if cancel_event is not None and cancel_event.is_set():
                raise WarmupCancelled("download cancelled")
            run_index = member_run_index(member_name)
            dest = dest_dir / f"run_{run_index:04d}.parquet"
            dest.write_bytes(archive.read(member_name))
            run_files[run_index] = dest

    record_event(
        "parquet_persisted",
        symbol=symbol,
        date=date,
        scenario=scenario,
        files=len(run_files),
    )
    if not run_files:
        raise ValueError(f"No sim_data.parquet returned for {symbol} {date} {scenario}")
    return cached, n_runs, run_files
