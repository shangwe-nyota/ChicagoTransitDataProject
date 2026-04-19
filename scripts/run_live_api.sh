#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/live_env.sh"

"${LIVE_PYTHON_BIN}" -m uvicorn dashboard.live_api:app --reload --host "${LIVE_API_HOST:-127.0.0.1}" --port "${LIVE_API_PORT:-8000}"
