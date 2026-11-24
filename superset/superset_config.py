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
        "font-src": ["'self'", "data:", "https://fonts.gstatic.com"],
        "worker-src": ["'self'", "blob:"],
        "connect-src": ["'self'", "https://bi.seanstock.dev"],
        "object-src": "'none'",
        "style-src": ["'self'", "'unsafe-inline'", "https://fonts.googleapis.com"],
        "script-src": ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
        "frame-ancestors": ["'self'", *ALLOWED_EMBED_ORIGINS],
    },
    "frame_options": None,  # Rely on CSP frame-ancestors so seanstock.dev can embed
    "force_https": False,  # TLS terminated by Cloudflare Tunnel
}

# Custom SeanStock.dev Themes (Ant Design v5 tokens matching seanstock.dev)
ENABLE_UI_THEME_ADMINISTRATION = True

SEANSTOCK_FONT_URLS = [
    "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap",
]

SEANSTOCK_DARK_THEME = {
    "algorithm": "dark",
    "token": {
        "brandAppName": "SeanStock.dev BI",
        "colorPrimary": "#22d3ee",
        "colorLink": "#22d3ee",
        "colorLinkHover": "#38bdf8",
        "colorInfo": "#38bdf8",
        "colorSuccess": "#10b981",
        "colorWarning": "#f59e0b",
        "colorError": "#f43f5e",
        "colorBgBase": "#090b10",
        "colorBgLayout": "#0d1119",
        "colorBgContainer": "#111622",
        "colorBgElevated": "#161d2c",
        "colorBorder": "#232c3d",
        "colorBorderSecondary": "#1c2433",
        "colorTextBase": "#f1f5f9",
        "colorText": "#f1f5f9",
        "colorTextSecondary": "#94a3b8",
        "colorTextTertiary": "#64748b",
        "borderRadius": 10,
        "borderRadiusLG": 14,
        "borderRadiusSM": 6,
        "fontFamily": "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
        "fontFamilyCode": "'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, monospace",
        "fontUrls": SEANSTOCK_FONT_URLS,
    },
}

THEME_DARK = SEANSTOCK_DARK_THEME

THEME_DEFAULT = {
    "algorithm": "default",
    "token": {
        "brandAppName": "SeanStock.dev BI",
        "colorPrimary": "#0284c7",
        "colorLink": "#0284c7",
        "colorSuccess": "#059669",
        "colorWarning": "#d97706",
        "colorError": "#e11d48",
        "colorBgBase": "#f8fafc",
        "colorBgLayout": "#f1f5f9",
        "colorBgContainer": "#ffffff",
        "colorTextBase": "#0f172a",
        "borderRadius": 10,
        "borderRadiusLG": 14,
        "fontFamily": "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
        "fontFamilyCode": "'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, monospace",
        "fontUrls": SEANSTOCK_FONT_URLS,
    },
}

EXTRA_CATEGORICAL_COLOR_SCHEMES = [
    {
        "id": "seanstock_palette",
        "description": "SeanStock.dev dark-first cyan & emerald analytics palette",
        "label": "SeanStock.dev",
        "isDefault": True,
        "colors": [
            "#22d3ee",
            "#10b981",
            "#38bdf8",
            "#a78bfa",
            "#f59e0b",
            "#fb7185",
            "#34d399",
            "#60a5fa",
            "#f472b6",
            "#94a3b8",
        ],
    }
]

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

# Native Superset 6.x Model Context Protocol (MCP) service (LAN-only NodePort, never public)
ENABLE_MCP_SERVICE = True
MCP_SERVICE_HOST = os.environ.get("MCP_SERVICE_HOST", "0.0.0.0")
MCP_SERVICE_PORT = int(os.environ.get("MCP_SERVICE_PORT", "5008"))
MCP_DEV_USERNAME = os.environ.get("MCP_DEV_USERNAME", "admin")
MCP_SERVICE_URL = os.environ.get("MCP_SERVICE_URL", "http://192.168.68.212:30508/mcp")

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
