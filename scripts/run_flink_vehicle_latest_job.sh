#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/live_env.sh"

FLINK_HOME="${FLINK_HOME:-/opt/homebrew/opt/apache-flink@1/libexec}"
DEFAULT_FLINK_KAFKA_CONNECTOR_JAR="${PROJECT_ROOT}/tools/flink/connectors/flink-sql-connector-kafka-3.3.0-1.20.jar"
FLINK_KAFKA_CONNECTOR_JAR="${FLINK_KAFKA_CONNECTOR_JAR:-${DEFAULT_FLINK_KAFKA_CONNECTOR_JAR}}"
FLINK_LOG_DIR="${FLINK_LOG_DIR:-${PROJECT_ROOT}/.flink/log}"
FLINK_PID_DIR="${FLINK_PID_DIR:-${PROJECT_ROOT}/.flink/pid}"
FLINK_EXECUTION_TARGET="${FLINK_EXECUTION_TARGET:-local}"
FLINK_PYTHON_RUNTIME_DIR="${FLINK_PYTHON_RUNTIME_DIR:-${PROJECT_ROOT}/.flink/python}"
CITY="${1:-${LIVE_CITY:-boston}}"

if [[ "${FLINK_KAFKA_CONNECTOR_JAR}" == "/absolute/path/to/flink-sql-connector-kafka-3.3.0-1.20.jar" ]]; then
  FLINK_KAFKA_CONNECTOR_JAR="${DEFAULT_FLINK_KAFKA_CONNECTOR_JAR}"
fi

if [[ ! -f "${FLINK_KAFKA_CONNECTOR_JAR}" && -f "${DEFAULT_FLINK_KAFKA_CONNECTOR_JAR}" ]]; then
  FLINK_KAFKA_CONNECTOR_JAR="${DEFAULT_FLINK_KAFKA_CONNECTOR_JAR}"
fi

bash "${PROJECT_ROOT}/scripts/prepare_flink_python.sh"

if [[ ! -f "$FLINK_KAFKA_CONNECTOR_JAR" ]]; then
  echo "Missing Kafka connector JAR: $FLINK_KAFKA_CONNECTOR_JAR"
  echo "Run: bash scripts/download_flink_connector.sh"
  exit 1
fi

if [[ ! -f "${FLINK_PYTHON_RUNTIME_DIR}/pyflink/bin/pyflink-udf-runner.sh" ]]; then
  echo "Missing extracted PyFlink runtime in ${FLINK_PYTHON_RUNTIME_DIR}"
  exit 1
fi

mkdir -p "$FLINK_LOG_DIR" "$FLINK_PID_DIR"

export FLINK_LOG_DIR
export FLINK_PID_DIR
export FLINK_KAFKA_CONNECTOR_JAR
export FLINK_HOME
export PYTHONPATH="${FLINK_PYTHON_RUNTIME_DIR}:${FLINK_PYTHON_RUNTIME_DIR}/py4j-src:${FLINK_PYTHON_RUNTIME_DIR}/cloudpickle-src:${PYTHONPATH:-}"

"$FLINK_HOME/bin/flink" run \
  --target "${FLINK_EXECUTION_TARGET}" \
  --python "${PROJECT_ROOT}/jobs/realtime/flink_vehicle_latest_job.py" \
  --pyFiles "file://${PROJECT_ROOT}/src,file://${PROJECT_ROOT}/jobs" \
  --pyClientExecutable "${LIVE_PYTHON_BIN}" \
  --pyExecutable "${LIVE_PYTHON_BIN}" \
  "${CITY}"
