#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/live_env.sh"

CONNECTOR_DIR="${PROJECT_ROOT}/tools/flink/connectors"
CONNECTOR_PATH="${CONNECTOR_DIR}/flink-sql-connector-kafka-3.3.0-1.20.jar"

mkdir -p "$CONNECTOR_DIR"

curl -L -o "$CONNECTOR_PATH" \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar

echo "Downloaded connector to $CONNECTOR_PATH"
