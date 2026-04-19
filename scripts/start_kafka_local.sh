#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/live_env.sh"

KAFKA_HOME="${KAFKA_HOME:-/opt/homebrew/opt/kafka}"
KAFKA_CONFIG="${KAFKA_CONFIG:-/opt/homebrew/etc/kafka/server.properties}"

if [[ ! -d "$KAFKA_HOME" ]]; then
  echo "Kafka is not installed at $KAFKA_HOME"
  exit 1
fi

if [[ ! -f "$KAFKA_CONFIG" ]]; then
  echo "Kafka config not found: $KAFKA_CONFIG"
  exit 1
fi

"$KAFKA_HOME/bin/kafka-server-start" "$KAFKA_CONFIG"
