#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
until pg_isready -h hive-metastore-db -p 5432 -U ${HIVE_METASTORE_USER:-hive}; do
  echo "Waiting for PostgreSQL to be ready..."
  sleep 2
done

# Initialize Airflow database if not already initialized
if ! airflow db check; then
    echo "Initializing Airflow database..."
    airflow db init
    
    echo "Creating admin user..."
    airflow users create \
        --username ${_AIRFLOW_WWW_USER_USERNAME:-admin} \
        --password ${_AIRFLOW_WWW_USER_PASSWORD:-admin} \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com
else
    echo "Airflow database already initialized."
    
    # Check if admin user exists, create if not
    if ! airflow users get --username ${_AIRFLOW_WWW_USER_USERNAME:-admin} 2>/dev/null; then
        echo "Creating admin user..."
        airflow users create \
            --username ${_AIRFLOW_WWW_USER_USERNAME:-admin} \
            --password ${_AIRFLOW_WWW_USER_PASSWORD:-admin} \
            --firstname Admin \
            --lastname User \
            --role Admin \
            --email admin@example.com
    else
        echo "Admin user already exists."
    fi
fi

# If no arguments are passed, default to showing help
if [ $# -eq 0 ]; then
    exec airflow --help
else
    # Handle Airflow subcommands
    if [ "$1" = "webserver" ] || [ "$1" = "scheduler" ]; then
        exec airflow "$@"
    else
        exec "$@"
    fi
fi