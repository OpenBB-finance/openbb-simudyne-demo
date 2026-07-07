"""Shared warmup status, event log, and logger."""

from __future__ import annotations

import logging
import threading
import time

WARMUP_EVENT_LIMIT = 1000

WARMUP_STATUS: dict = {
    "state": "idle",
    "started_at": None,
    "finished_at": None,
    "completed": 0,
    "total": 0,
    "current": None,
    "current_started_at": None,
    "error": None,
    "event_seq": 0,
    "events": [],
}

WARMUP_LOCK = threading.Lock()
WARMUP_CANCEL_EVENT = threading.Event()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOGGER = logging.getLogger("simudyne_duck")


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def record_event(message: str, level: str = "info", **fields) -> None:
    event = {"time": now_iso(), "level": level, "message": message}
    if fields:
        event["fields"] = fields

    with WARMUP_LOCK:
        WARMUP_STATUS["event_seq"] += 1
        event["seq"] = WARMUP_STATUS["event_seq"]
        WARMUP_STATUS["events"].append(event)
        if len(WARMUP_STATUS["events"]) > WARMUP_EVENT_LIMIT:
            WARMUP_STATUS["events"] = WARMUP_STATUS["events"][-WARMUP_EVENT_LIMIT:]

    if level == "error":
        LOGGER.error("%s | %s", message, fields if fields else "{}")
    elif level == "warning":
        LOGGER.warning("%s | %s", message, fields if fields else "{}")
    else:
        LOGGER.info("%s | %s", message, fields if fields else "{}")


class WarmupCancelled(Exception):
    """Raised to unwind the warmup pipeline when cancellation is requested."""
