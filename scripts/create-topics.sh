#!/bin/bash
set -euo pipefail
TOPICS=(
  MetadataChangeEvent_v1
  MetadataChangeProposal_v1
  MetadataAuditEvent_v4
  PlatformEvent_v1
  MetadataChangeLog_Versioned_v1
  MetadataChangeLog_Timeseries_v1
  DataHubUpgradeHistory_v1
)
for topic in "${TOPICS[@]}"; do
  /opt/bitnami/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server kafka:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic "$topic"
  echo "Ensured topic $topic"
done
echo "Kafka topics ready"
