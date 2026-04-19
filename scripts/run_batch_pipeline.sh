#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/run_batch_pipeline.sh all [--load-snowflake] [--force] [--run-id RUN_ID]
  bash scripts/run_batch_pipeline.sh chicago [--load-snowflake] [--force] [--run-id RUN_ID]
  bash scripts/run_batch_pipeline.sh boston [--load-snowflake] [--force] [--run-id RUN_ID]
  bash scripts/run_batch_pipeline.sh snowflake [--force] [--run-id RUN_ID]

Examples:
  bash scripts/run_batch_pipeline.sh all --load-snowflake
  bash scripts/run_batch_pipeline.sh chicago --run-id chicago-20260419T220000Z-demo
  bash scripts/run_batch_pipeline.sh snowflake --run-id airflow-20260419T000000
EOF
}

TARGET="${1:-}"
if [[ -z "${TARGET}" ]]; then
  usage
  exit 1
fi
shift

LOAD_SNOWFLAKE=false
FORCE=false
RUN_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --load-snowflake)
      LOAD_SNOWFLAKE=true
      ;;
    --force)
      FORCE=true
      ;;
    --run-id)
      shift
      RUN_ID="${1:-}"
      if [[ -z "${RUN_ID}" ]]; then
        echo "Missing value for --run-id" >&2
        exit 1
      fi
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="manual-$(date -u +%Y%m%dT%H%M%SZ)"
fi

run_city() {
  local city="$1"
  local cmd=(python jobs/pipeline/run_city_batch_pipeline.py --city "${city}" --run-id "${RUN_ID}")
  if [[ "${FORCE}" == "true" ]]; then
    cmd+=(--force)
  fi

  echo
  echo "Running ${city} batch pipeline"
  echo "Command: ${cmd[*]}"
  (cd "${ROOT_DIR}" && "${cmd[@]}")
}

run_snowflake() {
  local cmd=(python -m jobs.load.load_to_snowflake --run-id "${RUN_ID}")
  if [[ "${FORCE}" == "true" ]]; then
    cmd+=(--force)
  fi

  echo
  echo "Loading batch outputs to Snowflake"
  echo "Command: ${cmd[*]}"
  (cd "${ROOT_DIR}" && "${cmd[@]}")
}

case "${TARGET}" in
  all)
    run_city "chicago"
    run_city "boston"
    if [[ "${LOAD_SNOWFLAKE}" == "true" ]]; then
      run_snowflake
    fi
    ;;
  chicago|boston)
    run_city "${TARGET}"
    if [[ "${LOAD_SNOWFLAKE}" == "true" ]]; then
      run_snowflake
    fi
    ;;
  snowflake)
    run_snowflake
    ;;
  *)
    echo "Unknown target: ${TARGET}" >&2
    usage
    exit 1
    ;;
esac

echo
echo "Batch run complete. run_id=${RUN_ID}"
