#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -f "${PROJECT_ROOT}/.env" ]]; then
  set -a
  source "${PROJECT_ROOT}/.env"
  set +a
fi

if [[ -x "${PROJECT_ROOT}/.venv-live/bin/python" ]]; then
  export LIVE_PYTHON_BIN="${PROJECT_ROOT}/.venv-live/bin/python"
else
  export LIVE_PYTHON_BIN="${LIVE_PYTHON_BIN:-python}"
fi

export PROJECT_ROOT
