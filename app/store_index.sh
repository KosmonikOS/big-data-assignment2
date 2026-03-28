#!/bin/bash
set -e

source /app/.venv/bin/activate

python3 /app/load_index_to_cassandra.py