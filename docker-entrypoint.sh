#!/usr/bin/env sh
set -eu

if [ -z "${SIMUDYNE_API_KEY:-}" ]; then
  echo "[entrypoint] SIMUDYNE_API_KEY is required" >&2
  exit 1
fi

exec python simudyne_duck_app.py
