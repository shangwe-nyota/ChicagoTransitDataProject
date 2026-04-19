#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/live_env.sh"

CITY="${LIVE_CITY:-boston}"

if [[ "${1:-}" != "" && "${1:-}" != --* ]]; then
  CITY="$1"
  shift
fi

"${LIVE_PYTHON_BIN}" -m jobs.realtime.mbta_poll_to_kafka --city "${CITY}" --interval "${LIVE_POLL_INTERVAL_SECONDS:-5}" "$@"
