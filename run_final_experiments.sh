#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export SPARK_HOME="${SPARK_HOME:-}"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}"
export PYTHONPATH="$PROJECT_ROOT"

cd "$PROJECT_ROOT"

LIMIT="${1:-50000}"
EXTRA_ARGS=()

SPARK_CHECK=""
RESULTS_DIR="results/final"

if [[ "${2:-}" == "spark-check" ]]; then
    SPARK_CHECK="--spark-check"
fi

exec "$PROJECT_ROOT/.venv/bin/python" \
    "scripts/run_final_experiments.py" \
    --limit "$LIMIT" \
    --results-dir "$RESULTS_DIR" \
    ${SPARK_CHECK} \
    "$@"