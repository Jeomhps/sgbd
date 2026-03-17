#!/bin/bash

# Simple script to run unit tests
# Usage: ./test.sh [options]

# Activate virtual environment
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

# Run pytest with all arguments passed to this script
echo "🧪 Running unit tests..."
python3 -m pytest "$@"
echo "✅ Unit tests completed!"