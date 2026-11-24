FROM apache/superset:6.1.0

USER root

# Superset 6.x uses uv + /app/.venv for Python dependencies
RUN . /app/.venv/bin/activate && \
    uv pip install --no-cache \
      "duckdb>=1.1,<2.0" \
      "duckdb-engine>=0.13,<1.0" \
      "flask-cors>=4.0,<6.0" \
      "psycopg2-binary>=2.9,<3.0" \
      "redis>=5.0,<6.0"

COPY superset_config.py /app/pythonpath/superset_config.py
COPY init/ /app/init/
RUN chmod +x /app/init/init.sh

USER superset

ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
