"""
Apache Superset production configuration for seanstock.dev Live Showcase.

Tuned for single-node Kubernetes (<= 5GB total cluster budget):
  - Read-only DuckDB + Parquet warehouse mounted at /data
  - Redis query & dashboard caching
  - Embedded SDK + Guest Token support for https://seanstock.dev
  - Lightweight public telemetry endpoint at /api/v1/showcase/meta
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from flask import jsonify

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_METADATA_DB_URI"]

# Behind Cloudflare Tunnel / reverse proxy
ENABLE_PROXY_FIX = True
PROXY_FIX_CONFIG = {"x_for": 1, "x_proto": 1, "x_host": 1, "x_port": 1, "x_prefix": 1}

# Feature flags for clean Embedded SDK dashboards
FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": True,
    "DASHBOARD_RBAC": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "HORIZONTAL_FILTER_BAR": True,
}

# Guest Token settings (minted server-side by seanstock.dev Cloudflare Pages Function)
GUEST_ROLE_NAME = "Gamma"
GUEST_TOKEN_JWT_SECRET = os.environ.get("SUPERSET_GUEST_TOKEN_SECRET", SECRET_KEY)
GUEST_TOKEN_JWT_ALGO = "HS256"
GUEST_TOKEN_HEADER_NAME = "X-GuestToken"
GUEST_TOKEN_JWT_EXP_SECONDS = 300  # 5 minutes

# Allow embedding strictly on seanstock.dev (+ local preview during dev)
ALLOWED_EMBED_ORIGINS = [
    "https://seanstock.dev",
    "https://www.seanstock.dev",
    "https://*.seanstock-dev.pages.dev",
    "http://localhost:3000",
    "http://localhost:8788",
]

ENABLE_CORS = True
CORS_OPTIONS = {
    "supports_credentials": True,
    "allow_headers": ["*"],
    "resources": ["/api/*", "/superset/*"],
    "origins": ALLOWED_EMBED_ORIGINS,
}

# Frame ancestors & security headers (allow embedding strictly on seanstock.dev)
HTTP_HEADERS = {}
TALISMAN_ENABLED = True
TALISMAN_CONFIG = {
    "content_security_policy": {
        "default-src": ["'self'"],
        "img-src": ["'self'", "blob:", "data:"],
        "font-src": ["'self'", "data:"],
        "worker-src": ["'self'", "blob:"],
        "connect-src": ["'self'", "https://bi.seanstock.dev"],
        "object-src": "'none'",
        "style-src": ["'self'", "'unsafe-inline'"],
        "script-src": ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
        "frame-ancestors": ["'self'", *ALLOWED_EMBED_ORIGINS],
    },
    "frame_options": None,  # Rely on CSP frame-ancestors so seanstock.dev can embed
    "force_https": False,  # TLS terminated by Cloudflare Tunnel
}

# Redis Cache (keeps repeated portfolio visits fast and CPU idle)
# Note: Kubernetes auto-injects REDIS_PORT=tcp://<ip>:6379 for Services named "redis",
# so read CACHE_REDIS_PORT first and defensively extract the trailing port number.
REDIS_HOST = os.environ.get("CACHE_REDIS_HOST") or os.environ.get("REDIS_HOST", "redis")
_raw_redis_port = os.environ.get("CACHE_REDIS_PORT") or os.environ.get("REDIS_PORT", "6379")
REDIS_PORT = int(str(_raw_redis_port).rsplit(":", 1)[-1])

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_meta_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 0,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 900,  # 15 minutes (half of Meltano 30-min cadence)
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 1,
}

FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 3600,
    "CACHE_KEY_PREFIX": "superset_filter_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 2,
}

EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 3600,
    "CACHE_KEY_PREFIX": "superset_explore_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 3,
}

# Restrict row limits to protect small VM memory
ROW_LIMIT = 50000
SQL_MAX_ROW = 50000
SUPERSET_WEBSERVER_TIMEOUT = 60

# Register a lightweight, unauthenticated read-only endpoint for pipeline-meta.json
# so seanstock.dev can display live Meltano sync telemetry without an extra sidecar.
def FLASK_APP_MUTATOR(app):  # noqa: N802
    meta_path = Path(os.environ.get("DATA_DIR", "/data")) / "gold" / "pipeline-meta.json"

    @app.route("/api/v1/showcase/meta", methods=["GET"])
    def showcase_pipeline_meta():
        if not meta_path.exists():
            return jsonify({"status": "initializing", "message": "Waiting for first Meltano run"}), 503
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception as exc:
            return jsonify({"status": "error", "message": str(exc)}), 500
        resp = jsonify(payload)
        resp.headers["Cache-Control"] = "public, max-age=60"
        return resp
