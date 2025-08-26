#!/bin/bash
# run_demo.sh - End-to-end demo for the e-commerce microservices stack

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_SCRIPT="$REPO_ROOT/scripts/run_demo.py"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is required but not installed."
    exit 1
fi

# Check if the Python script exists
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "Error: Python demo script not found at $PYTHON_SCRIPT"
    exit 1
fi

# Run the Python demo script
echo "Starting E-commerce Microservices Demo"
echo "=================================================="
python3 "$PYTHON_SCRIPT"