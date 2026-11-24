#!/usr/bin/env bash
set -euo pipefail

echo "[superset-init] Upgrading Superset metadata DB..."
superset db upgrade

echo "[superset-init] Ensuring admin user exists..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER:-admin}" \
  --firstname "Sean" \
  --lastname "Stock" \
  --email "${SUPERSET_ADMIN_EMAIL:-sean.stock@hotmail.com}" \
  --password "${SUPERSET_ADMIN_PASSWORD}" || true

if [ -n "${SUPERSET_EMBED_USER:-}" ] && [ -n "${SUPERSET_EMBED_PASSWORD:-}" ]; then
  echo "[superset-init] Ensuring dedicated embed token service account exists..."
  superset fab create-user \
    --role "Admin" \
    --username "${SUPERSET_EMBED_USER}" \
    --firstname "Embed" \
    --lastname "Service" \
    --email "embed@seanstock.dev" \
    --password "${SUPERSET_EMBED_PASSWORD}" || true
fi

echo "[superset-init] Initializing roles and permissions..."
superset init

echo "[superset-init] Bootstrapping DuckDB warehouse connection & Gold views..."
python3 /app/init/bootstrap.py

echo "[superset-init] Done."
