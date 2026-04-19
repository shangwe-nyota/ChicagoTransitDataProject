#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/live_env.sh"

FLINK_HOME="${FLINK_HOME:-/opt/homebrew/opt/apache-flink@1/libexec}"
FLINK_PYTHON_RUNTIME_DIR="${FLINK_PYTHON_RUNTIME_DIR:-${PROJECT_ROOT}/.flink/python}"
FLINK_PYTHON_ZIP="${FLINK_HOME}/opt/python/pyflink.zip"

PY4J_SOURCE="$(find "${FLINK_HOME}/opt/python" -maxdepth 1 -name 'py4j-*-src.zip' | head -n 1)"
CLOUDPICKLE_SOURCE="$(find "${FLINK_HOME}/opt/python" -maxdepth 1 -name 'cloudpickle-*-src.zip' | head -n 1)"

if [[ ! -f "${FLINK_PYTHON_ZIP}" ]]; then
  echo "Missing PyFlink zip: ${FLINK_PYTHON_ZIP}"
  exit 1
fi

if [[ -z "${PY4J_SOURCE}" || ! -f "${PY4J_SOURCE}" ]]; then
  echo "Missing Py4J source zip under ${FLINK_HOME}/opt/python"
  exit 1
fi

if [[ -z "${CLOUDPICKLE_SOURCE}" || ! -f "${CLOUDPICKLE_SOURCE}" ]]; then
  echo "Missing cloudpickle source zip under ${FLINK_HOME}/opt/python"
  exit 1
fi

mkdir -p "${FLINK_PYTHON_RUNTIME_DIR}"

if [[ ! -f "${FLINK_PYTHON_RUNTIME_DIR}/pyflink/bin/pyflink-udf-runner.sh" ]]; then
  rm -rf \
    "${FLINK_PYTHON_RUNTIME_DIR}/pyflink" \
    "${FLINK_PYTHON_RUNTIME_DIR}/py4j-src" \
    "${FLINK_PYTHON_RUNTIME_DIR}/cloudpickle-src"

  unzip -q "${FLINK_PYTHON_ZIP}" -d "${FLINK_PYTHON_RUNTIME_DIR}"
  unzip -q "${PY4J_SOURCE}" -d "${FLINK_PYTHON_RUNTIME_DIR}/py4j-src"
  unzip -q "${CLOUDPICKLE_SOURCE}" -d "${FLINK_PYTHON_RUNTIME_DIR}/cloudpickle-src"
fi

chmod +x "${FLINK_PYTHON_RUNTIME_DIR}/pyflink/bin/pyflink-udf-runner.sh"

echo "Prepared PyFlink runtime in ${FLINK_PYTHON_RUNTIME_DIR}"
