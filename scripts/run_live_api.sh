#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/live_env.sh"

python_supports_batch_api() {
  local python_bin="$1"
  "${python_bin}" -c "import cryptography, fastapi, pandas, redis, snowflake.connector, uvicorn, websockets" >/dev/null 2>&1
}

API_PYTHON_BIN="${LIVE_API_PYTHON_BIN:-${LIVE_PYTHON_BIN}}"
LIVE_API_RELOAD="${LIVE_API_RELOAD:-false}"

if ! python_supports_batch_api "${API_PYTHON_BIN}"; then
  if command -v python >/dev/null 2>&1 && python_supports_batch_api python; then
    echo "Batch dashboard dependencies are not available in ${API_PYTHON_BIN}; falling back to base python for FastAPI."
    API_PYTHON_BIN="python"
  else
    echo "Could not find a Python interpreter with the batch dashboard dependencies required by dashboard.live_api:app" >&2
    echo "Install pandas + Snowflake deps into .venv-live or set LIVE_API_PYTHON_BIN to a working interpreter." >&2
    exit 1
  fi
fi

UVICORN_ARGS=(
  dashboard.live_api:app
  --host "${LIVE_API_HOST:-127.0.0.1}"
  --port "${LIVE_API_PORT:-8000}"
)

case "${LIVE_API_RELOAD}" in
  1|true|TRUE|yes|YES)
    UVICORN_ARGS+=(--reload)
    ;;
esac

"${API_PYTHON_BIN}" -m uvicorn "${UVICORN_ARGS[@]}"
