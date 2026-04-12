#!/bin/bash
# Creates the Airflow metadata database alongside the pipeline database.
# Runs once on first postgres container start via docker-entrypoint-initdb.d.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow;
    CREATE USER airflow WITH PASSWORD 'airflow';
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
EOSQL

# PostgreSQL 15+ revoked CREATE on public schema by default.
# Re-grant it so Airflow can create its metadata tables.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "airflow" <<-EOSQL
    GRANT ALL ON SCHEMA public TO airflow;
EOSQL
