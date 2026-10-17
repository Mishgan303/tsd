#!/usr/bin/env bash
set -euo pipefail

superset db upgrade

if superset fab list-users | grep -Fq "${SUPERSET_ADMIN_USERNAME}"; then
  echo "Superset admin already exists"
else
  superset fab create-admin \
    --username "${SUPERSET_ADMIN_USERNAME}" \
    --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Lab}" \
    --lastname "${SUPERSET_ADMIN_LASTNAME:-Admin}" \
    --email "${SUPERSET_ADMIN_EMAIL:-admin@example.local}" \
    --password "${SUPERSET_ADMIN_PASSWORD}"
fi

superset init

