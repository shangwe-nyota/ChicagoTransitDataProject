#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/live_env.sh"

FLINK_HOME="${FLINK_HOME:-/opt/homebrew/opt/apache-flink@1/libexec}"
FLINK_LOG_DIR="${FLINK_LOG_DIR:-${PROJECT_ROOT}/.flink/log}"
FLINK_PID_DIR="${FLINK_PID_DIR:-${PROJECT_ROOT}/.flink/pid}"

if [[ ! -d "$FLINK_HOME" ]]; then
  echo "Flink 1.20 is not installed at $FLINK_HOME"
  exit 1
fi

mkdir -p "$FLINK_LOG_DIR" "$FLINK_PID_DIR"

export FLINK_LOG_DIR
export FLINK_PID_DIR

"$FLINK_HOME/bin/start-cluster.sh"
