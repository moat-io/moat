#!/bin/bash
set -e

echo "entrypoint.sh: $@"

run_migrations() {
    echo "Running migrations..."
    alembic -c /app/moat/alembic.ini upgrade head
}

start_server() {
    echo "Starting server on port 8000..."
    exec uwsgi --http 0.0.0.0:8000 --die-on-term --master -p 4 -w src.uwsgi:app
}

seed_db() {
    echo "Seeding database..."
    python src/_scripts/seed_db.py
}

if [ "$1" = "start-server" ]; then
    start_server

# tests
elif [ "$1" = "test" ]; then
  if [ "$2" = "opa" ]; then
    opa test /app/opa/trino -v
  elif [ "$2" == "unit" ]; then
    cd /app && python -m pytest
  fi

# DB migrations
elif [ "$1" = "migrate" ]; then
  if [ "$2" = "upgrade" ]; then
    run_migrations
  elif [ "$2" == "revision" ]; then
    alembic -c /app/moat/alembic.ini revision --autogenerate -m "$3"
  fi

elif [ "$1" = "start-demo-server" ]; then
    run_migrations
    seed_db
    start_server

elif [ "$1" = "start-mock-server" ]; then
    python src/_scripts/mock_apis.py

# Default: Run the CLI command
else
    echo "Running CLI command: $@"
    exec python -m cli.src.cli "$@"
fi