#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/live_env.sh"

cd "${PROJECT_ROOT}/dashboard/web"
npm run dev -- --host 127.0.0.1 --port 5173 --strictPort
