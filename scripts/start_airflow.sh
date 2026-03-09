#!/usr/bin/env bash
set -e

airflow standalone &
AIRFLOW_PID=$!

echo "Waiting for Airflow to initialize..."

until airflow db check; do
  echo "Database not ready yet..."
  sleep 3
done

echo "Database ready"

echo "Creating admin with credentials: login=levon;password=12345"
airflow users create \
  --username levon \
  --firstname Levon \
  --lastname Khorasandzhian \
  --role Admin \
  --email lakhorasandzhyan@edu.hse.ru \
  --password 12345 || true

wait $AIRFLOW_PID