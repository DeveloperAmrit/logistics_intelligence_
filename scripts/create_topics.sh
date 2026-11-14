#!/usr/bin/env bash
# Create every topic with the partition count and retention from the design.
set -euo pipefail
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
PARTITIONS="${PARTITIONS:-6}"
RETENTION_MS=$((7 * 24 * 60 * 60 * 1000))   # seven days

TOPICS=(raw.gps raw.sensor raw.warehouse raw.shipment
        alerts.cold_chain alerts.route_deviation alerts.delay_risk quarantine.dlq)

for topic in "${TOPICS[@]}"; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$PARTITIONS" --replication-factor 1 \
    --config "retention.ms=$RETENTION_MS"
  echo "created $topic"
done

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --list
