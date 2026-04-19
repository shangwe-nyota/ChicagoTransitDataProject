#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/live_env.sh"

RUNTIME_DIR="${PROJECT_ROOT}/.live"
PID_DIR="${RUNTIME_DIR}/pids"
LOG_DIR="${RUNTIME_DIR}/logs"
CITY="${2:-${LIVE_CITY:-boston}}"
KAFKA_HOME="${KAFKA_HOME:-/opt/homebrew/opt/kafka}"
KAFKA_CONFIG="${KAFKA_CONFIG:-/opt/homebrew/etc/kafka/server.properties}"

mkdir -p "${PID_DIR}" "${LOG_DIR}"

is_port_listening() {
  local port="$1"
  (echo >/dev/tcp/127.0.0.1/"${port}") >/dev/null 2>&1
}

kill_listeners_on_port() {
  local port="$1"
  local label="$2"
  local pids

  pids="$(lsof -tiTCP:"${port}" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -z "${pids}" ]]; then
    return 0
  fi

  echo "Stopping ${label} listener(s) on port ${port}: ${pids}"
  kill ${pids} >/dev/null 2>&1 || true
  sleep 1

  local stubborn_pids
  stubborn_pids="$(lsof -tiTCP:"${port}" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "${stubborn_pids}" ]]; then
    kill -9 ${stubborn_pids} >/dev/null 2>&1 || true
  fi
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local timeout_seconds="${3:-20}"
  local started_at
  started_at="$(date +%s)"

  while ! (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; do
    if (( "$(date +%s)" - started_at >= timeout_seconds )); then
      echo "Timed out waiting for ${host}:${port}"
      return 1
    fi
    sleep 1
  done
}

is_pid_running() {
  local pid_file="$1"
  if [[ ! -f "${pid_file}" ]]; then
    return 1
  fi

  local pid
  pid="$(cat "${pid_file}")"
  kill -0 "${pid}" >/dev/null 2>&1
}

start_background() {
  local name="$1"
  shift
  local pid_file="${PID_DIR}/${name}.pid"
  local log_file="${LOG_DIR}/${name}.log"

  if is_pid_running "${pid_file}"; then
    echo "${name} is already running with PID $(cat "${pid_file}")"
    return 0
  fi

  nohup "$@" >"${log_file}" 2>&1 &
  echo $! > "${pid_file}"
  sleep 2

  if is_pid_running "${pid_file}"; then
    echo "Started ${name} with PID $(cat "${pid_file}")"
    return 0
  fi

  echo "${name} exited during startup. Recent log output:"
  tail -n 20 "${log_file}" || true
  rm -f "${pid_file}"
  return 1
}

start_background_if_port_free() {
  local name="$1"
  local port="$2"
  shift 2

  if is_port_listening "${port}"; then
    echo "${name} already has something listening on ${port}; skipping startup"
    return 0
  fi

  start_background "${name}" "$@"

  wait_for_port 127.0.0.1 "${port}" 15
}

cleanup_stale_pid_files() {
  for pid_file in "${PID_DIR}"/*.pid; do
    [[ -e "${pid_file}" ]] || continue
    if ! is_pid_running "${pid_file}"; then
      rm -f "${pid_file}"
    fi
  done
}

stop_pid_file() {
  local name="$1"
  local pid_file="${PID_DIR}/${name}.pid"

  if ! is_pid_running "${pid_file}"; then
    rm -f "${pid_file}"
    return 0
  fi

  local pid
  pid="$(cat "${pid_file}")"
  kill "${pid}" >/dev/null 2>&1 || true
  sleep 1
  if kill -0 "${pid}" >/dev/null 2>&1; then
    kill -9 "${pid}" >/dev/null 2>&1 || true
  fi
  rm -f "${pid_file}"
  echo "Stopped ${name}"
}

start_infra() {
  echo "Starting live infrastructure for ${CITY}"

  if ! is_port_listening 6379; then
    redis-server --daemonize yes
    wait_for_port 127.0.0.1 6379
    echo "Redis is running on 6379"
  else
    echo "Redis is already running on 6379"
  fi

  if ! is_port_listening 9092; then
    "${KAFKA_HOME}/bin/kafka-server-start" -daemon "${KAFKA_CONFIG}"
    wait_for_port 127.0.0.1 9092
    echo "Kafka is running on 9092"
  else
    echo "Kafka is already running on 9092"
  fi

  bash "${PROJECT_ROOT}/scripts/create_live_topics.sh" "${CITY}"
}

start_stream() {
  echo "Starting live stream processors for ${CITY}"

  case "${CITY}" in
    boston)
      POLLER_NAME="mbta_to_kafka_${CITY}"
      POLLER_SCRIPT="${PROJECT_ROOT}/scripts/run_mbta_to_kafka.sh"
      ;;
    chicago)
      POLLER_NAME="cta_to_kafka_${CITY}"
      POLLER_SCRIPT="${PROJECT_ROOT}/scripts/run_cta_to_kafka.sh"
      ;;
    *)
      echo "No live poller defined for city: ${CITY}"
      exit 1
      ;;
  esac

  start_background "flink_latest_${CITY}" bash "${PROJECT_ROOT}/scripts/run_flink_vehicle_latest_job.sh" "${CITY}"
  sleep 2
  start_background "kafka_to_redis_${CITY}" bash "${PROJECT_ROOT}/scripts/run_kafka_latest_to_redis.sh" "${CITY}"
  start_background "${POLLER_NAME}" bash "${POLLER_SCRIPT}" "${CITY}"
}

start_app() {
  echo "Starting live app services"
  start_background_if_port_free "live_api" 8000 bash "${PROJECT_ROOT}/scripts/run_live_api.sh"
  start_background_if_port_free "live_dashboard" 5173 bash "${PROJECT_ROOT}/scripts/run_live_dashboard.sh"
  echo
  echo "Live links"
  echo "  UI:      http://127.0.0.1:5173"
  echo "  API:     http://127.0.0.1:8000"
  echo "  Health:  http://127.0.0.1:8000/api/live/${CITY}/health"
  echo
  echo "Useful commands"
  echo "  bash scripts/live.sh status"
  echo "  bash scripts/live.sh logs"
  echo "  bash scripts/live.sh down"
}

show_status() {
  cleanup_stale_pid_files
  echo "Live runtime status"
  for pid_file in "${PID_DIR}"/*.pid; do
    [[ -e "${pid_file}" ]] || continue
    local name
    name="$(basename "${pid_file}" .pid)"
    if is_pid_running "${pid_file}"; then
      echo "  ${name}: running (PID $(cat "${pid_file}"))"
    else
      echo "  ${name}: stale pid file"
    fi
  done

  if is_port_listening 6379; then
    echo "  redis: listening on 6379"
  else
    echo "  redis: not running"
  fi

  if is_port_listening 9092; then
    echo "  kafka: listening on 9092"
  else
    echo "  kafka: not running"
  fi

  if is_port_listening 8000; then
    echo "  api: listening on 8000"
  else
    echo "  api: not running"
  fi

  if is_port_listening 5173; then
    echo "  dashboard: listening on 5173"
  else
    echo "  dashboard: not running"
  fi
}

stop_all() {
  echo "Stopping live runtime"
  cleanup_stale_pid_files
  for pid_file in "${PID_DIR}"/*.pid; do
    [[ -e "${pid_file}" ]] || continue
    stop_pid_file "$(basename "${pid_file}" .pid)"
  done

  if is_port_listening 9092; then
    "${KAFKA_HOME}/bin/kafka-server-stop" >/dev/null 2>&1 || true
    echo "Requested Kafka shutdown"
  fi

  if is_port_listening 6379; then
    redis-cli shutdown >/dev/null 2>&1 || true
    echo "Requested Redis shutdown"
  fi

  kill_listeners_on_port 5173 "dashboard"
  kill_listeners_on_port 8000 "API"
}

show_logs() {
  local name="${2:-}"

  if [[ -n "${name}" ]]; then
    local log_file="${LOG_DIR}/${name}.log"
    if [[ ! -f "${log_file}" ]]; then
      echo "No log file found for ${name}"
      exit 1
    fi
    tail -n 60 "${log_file}"
    return 0
  fi

  echo "Available logs in ${LOG_DIR}"
  ls -1 "${LOG_DIR}" 2>/dev/null || true
}

usage() {
  cat <<EOF
Usage: bash scripts/live.sh <command>

Commands:
  infra   Start Redis, start Kafka, and create city topics for the target city
  stream  Start Flink latest job, Kafka-to-Redis updater, and the city poller
  app     Start the FastAPI backend and React dashboard
  all     Start infra, stream, and app for the target city
  down    Stop the live runtime processes started by this script
  logs    List live logs or tail one log via: bash scripts/live.sh logs <name>
  status  Show current live runtime status

Logs:
  ${LOG_DIR}
EOF
}

COMMAND="${1:-status}"

case "${COMMAND}" in
  infra)
    start_infra
    ;;
  stream)
    start_stream
    ;;
  app)
    start_app
    ;;
  all)
    start_infra
    start_stream
    start_app
    ;;
  down)
    stop_all
    ;;
  logs)
    show_logs "$@"
    ;;
  status)
    show_status
    ;;
  *)
    usage
    exit 1
    ;;
esac
