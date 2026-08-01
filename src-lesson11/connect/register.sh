#!/bin/sh
set -eu

curl -fsS --retry 12 --retry-all-errors --retry-delay 2 \
  -X PUT -H 'Content-Type: application/json' \
  --data-binary @/config/orders-connector.json \
  http://connect:8083/connectors/orders-cdc/config >/dev/null

attempt=0
while [ "$attempt" -lt 60 ]; do
  status="$(curl -fsS http://connect:8083/connectors/orders-cdc/status || true)"
  if echo "$status" | grep -q '"connector":{"state":"RUNNING"' \
     && echo "$status" | grep -q '"tasks":\[{"id":0,"state":"RUNNING"'; then
    echo "Debezium connector and task are RUNNING"
    exit 0
  fi
  if echo "$status" | grep -q '"state":"FAILED"'; then
    echo "Debezium failed: $status" >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  sleep 2
done

echo "Timed out waiting for Debezium: $status" >&2
exit 1
