#!/usr/bin/env bash
# Apply migrations in order. Safe to re-run: every statement is idempotent.
set -euo pipefail
for file in db/migrations/*.sql; do
  echo "applying $file"
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U logistics -d logistics < "$file"
done
echo "migrations applied"
