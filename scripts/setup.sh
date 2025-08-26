#!/bin/bash
# setup.sh — create venv and install all services in editable mode

set -e

SERVICES=("auth" "catalog" "order" "cart" "payment" "shipping" "notifications")
VENV_DIR="${1:-.venv}"
PYTHON="${2:-python3}"
SKIP_INSTALL=false

# Parse arguments
for arg in "$@"; do
    case $arg in
        --skip-install)
            SKIP_INSTALL=true
            shift
            ;;
        --venv=*)
            VENV_DIR="${arg#*=}"
            shift
            ;;
        --python=*)
            PYTHON="${arg#*=}"
            shift
            ;;
    esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PATH="$REPO_ROOT/$VENV_DIR"

echo ">>> Creating venv at $VENV_PATH (if missing)"
if [ ! -d "$VENV_PATH" ]; then
    "$PYTHON" -m venv "$VENV_PATH"
fi

# Determine OS-specific paths
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    PIP="$VENV_PATH/Scripts/pip.exe"
    PYTHON="$VENV_PATH/Scripts/python.exe"
    ACTIVATE="$VENV_PATH/Scripts/Activate.ps1"
else
    PIP="$VENV_PATH/bin/pip"
    PYTHON="$VENV_PATH/bin/python"
    ACTIVATE="$VENV_PATH/bin/activate"
fi

echo ">>> Upgrading pip"
"$PIP" install -U pip

# Optional: dev tools (uncomment if you want)
# "$PIP" install pre-commit ruff pytest

if [ "$SKIP_INSTALL" = false ]; then
    for service in "${SERVICES[@]}"; do
        PROJ="$REPO_ROOT/services/$service/pyproject.toml"
        if [ -f "$PROJ" ]; then
            echo ">>> Installing services/$service (editable)"
            "$PIP" install -e "$REPO_ROOT/services/$service"
        fi
    done
fi

echo -e "\nSetup complete."
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    echo "Activate your venv with:"
    echo "  $ACTIVATE"
else
    echo "Activate your venv with:"
    echo "  source $ACTIVATE"
fi