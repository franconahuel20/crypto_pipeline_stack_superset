#!/usr/bin/env bash
set -euo pipefail

# Esperar a Postgres (opcional pero útil)
until (echo > /dev/tcp/postgres/5432) >/dev/null 2>&1; do
  echo "Waiting for postgres:5432..."
  sleep 2
done


# Inicializaciones de Superset (descomenta si usás DB interna de superset)
# superset db upgrade || true
# superset fab create-admin \
#   --username "${SUPERSET_ADMIN:-admin}" \
#   --firstname Admin --lastname User \
#   --email "${SUPERSET_EMAIL:-admin@example.com}" \
#   --password "${SUPERSET_PASSWORD:-admin}" || true
# superset init || true

# Arrancar el servidor
exec gunicorn --bind 0.0.0.0:8088 --workers 4 "superset.app:create_app()"
