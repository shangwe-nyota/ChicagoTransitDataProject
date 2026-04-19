#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "$0")/live_env.sh"

KAFKA_HOME="${KAFKA_HOME:-/opt/homebrew/opt/kafka}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPIC_PREFIX="${KAFKA_TOPIC_PREFIX:-transit.live}"
CITY="${1:-${LIVE_CITY:-boston}}"

RAW_TOPIC="${TOPIC_PREFIX}.raw.${CITY}.vehicles"
LATEST_TOPIC="${TOPIC_PREFIX}.latest.${CITY}.vehicles"

"$KAFKA_HOME/bin/kafka-topics" --bootstrap-server "$BOOTSTRAP_SERVERS" --create --if-not-exists --topic "$RAW_TOPIC" --partitions 1 --replication-factor 1
"$KAFKA_HOME/bin/kafka-topics" --bootstrap-server "$BOOTSTRAP_SERVERS" --create --if-not-exists --topic "$LATEST_TOPIC" --partitions 1 --replication-factor 1

echo "Created topics for ${CITY}:"
echo "  $RAW_TOPIC"
echo "  $LATEST_TOPIC"
