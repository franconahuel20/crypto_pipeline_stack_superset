
# Crypto Pipeline v2 — Airflow + dbt + Superset (Docker)

Cambios clave para Windows:
- Sin `version:` en compose.
- `ENV` en Dockerfiles **en una sola línea** (sin `\`).
- Normalización de CRLF en scripts y sin `chmod` peligrosos bajo usuario sin permisos.

## Uso
```bash
cp .env.example .env
docker compose up -d --build
```

- Airflow: http://localhost:8080 (admin/admin)
- Superset: http://localhost:8090 (admin/admin)
