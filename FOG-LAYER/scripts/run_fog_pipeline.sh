#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VENV_DIR="$ROOT_DIR/.venv"
ENV_FILE="$ROOT_DIR/config/.env"
ENV_EXAMPLE_FILE="$ROOT_DIR/config/.env.example"

if [[ ! -d "$VENV_DIR" ]]; then
  echo "[error] Missing virtualenv at $VENV_DIR"
  echo "Create it first:"
  echo "  cd $ROOT_DIR && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
  exit 1
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
else
  echo "[warn] $ENV_FILE not found; using current shell env vars."
  echo "[info] Copy $ENV_EXAMPLE_FILE to $ENV_FILE and update values."
fi

exec python "$ROOT_DIR/src/fog_influx_logreg_pipeline.py" "$@"
