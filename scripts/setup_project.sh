#!/bin/bash

set -e

echo "Starting containers..."
docker compose up -d --build

echo "Check containers:"
docker ps

echo "Waiting for PostgreSQL..."

until docker exec postgres pg_isready -U levon > /dev/null 2>&1
do
  echo "PostgreSQL is not ready yet..."
  sleep 3
done

echo "PostgreSQL is ready!"


echo "Waiting for MongoDB..."

until docker exec mongo mongosh --eval "db.adminCommand('ping')" > /dev/null 2>&1
do
  echo "MongoDB is not ready yet..."
  sleep 3
done

echo "MongoDB is ready!"


echo "Generating MongoDB data..."
docker exec airflow python /opt/airflow/scripts/generate_data.py
echo "MongoDB data has been generated!"

echo "Creating PostgreSQL tables..."
docker exec -i postgres psql -U levon -d etl_db < sql/create_tables.sql
echo "PostgreSQL tables have been created!"

echo "---------------------------------------------"
echo "|  Project setup completed successfully!!!  |"
echo "---------------------------------------------"