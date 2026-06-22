#!/usr/bin/env sh
# Registers (or updates) the Debezium Postgres connector that captures changes
# on data_engineering.public.users_old. Run from the host after the stack is up:
#   sh kafka/register-connector.sh
#
# Uses the Kafka Connect REST API exposed on localhost:8083.

set -e

CONNECT_URL="http://localhost:8083"
CONFIG_FILE="$(dirname "$0")/users-connector.json"

echo "Waiting for Kafka Connect to be ready at ${CONNECT_URL}..."
until curl -s -o /dev/null "${CONNECT_URL}/connectors"; do
  sleep 2
done

echo "Registering Debezium connector..."
curl -s -X POST \
  -H "Content-Type: application/json" \
  --data @"${CONFIG_FILE}" \
  "${CONNECT_URL}/connectors" | tee /dev/stderr | grep -q '"name"' \
  || echo "(connector may already exist — check status below)"

echo
echo "Connector status:"
curl -s "${CONNECT_URL}/connectors/users-old-connector/status"
echo
