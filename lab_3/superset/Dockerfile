ARG SUPERSET_BASE_IMAGE=apache/superset:6.0.0
FROM ${SUPERSET_BASE_IMAGE}

USER root
RUN uv pip install \
    --python /app/.venv/bin/python \
    --no-cache \
    psycopg2-binary==2.9.10
USER superset

