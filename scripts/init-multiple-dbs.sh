#!/bin/bash
# Creates the extra "gold" database on top of the default "airflow" db
# that POSTGRES_DB already provisions. Runs once, on first container init.
set -e

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    IFS=',' read -ra DBS <<< "$POSTGRES_MULTIPLE_DATABASES"
    for db in "${DBS[@]}"; do
        if [ "$db" != "$POSTGRES_DB" ]; then
            echo "Creating database: $db"
            psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
                SELECT 'CREATE DATABASE $db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db')\gexec
EOSQL
        fi
    done
fi
