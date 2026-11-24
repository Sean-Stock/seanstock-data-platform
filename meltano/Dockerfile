FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/data \
    LOOKBACK_HOURS=48

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run via Meltano job orchestration (falls back cleanly to direct runner if needed)
CMD ["meltano", "--environment=prod", "run", "uk-grid-refresh"]
