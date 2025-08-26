#!/bin/bash
# seed.sh — run Alembic migrations for local dev

set -e

SERVICES=("auth" "catalog" "order" "shipping")

read_env_value() {
    local dotenv="$1"
    local key="$2"
    if [ ! -f "$dotenv" ]; then
        return
    fi
    grep -E "^\s*$key\s*=" "$dotenv" | head -1 | sed -E "s/^[^=]*=\s*['\"]?(.*?)['\"]?\s*$/\1/"
}

ensure_dsn() {
    local repo_root="$1"
    local dsn="${POSTGRES_DSN}"
    
    if [ -z "$dsn" ]; then
        dsn=$(read_env_value "$repo_root/deploy/.env" "POSTGRES_DSN")
    fi
    
    if [ -z "$dsn" ]; then
        dsn="postgresql+psycopg://postgres:postgres@localhost:5432/appdb"
    fi
    
    # make docker hostname usable from host when running alembic locally
    dsn=$(echo "$dsn" | sed 's/@postgres:/@localhost:/')
    export POSTGRES_DSN="$dsn"
    echo "$dsn"
}

run_alembic() {
    local service_dir="$1"
    if [ ! -f "$service_dir/alembic.ini" ]; then
        echo "Skipping $(basename "$service_dir"): no alembic.ini"
        return
    fi
    echo ">>> Migrating $(basename "$service_dir")"
    (cd "$service_dir" && alembic upgrade head)
}

main() {
    local services=("${@:-${SERVICES[@]}}")
    REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    
    dsn=$(ensure_dsn "$REPO_ROOT")
    echo "Using POSTGRES_DSN = $dsn"
    echo ">>> Running Alembic migrations"
    
    for service in "${services[@]}"; do
        run_alembic "$REPO_ROOT/services/$service"
    done
    
    echo "Done."
}

main "$@"