# SeanStock Data Platform — Live UK Grid & North West Energy Mix

Self-hosted mini data platform powering the live embedded analytics showcase on [seanstock.dev](https://seanstock.dev).

Built with **Meltano**, **DuckDB**, **Parquet (Bronze → Silver → Gold)**, **Apache Superset**, **Redis**, **PostgreSQL**, and **Cloudflare Tunnel**, orchestrated on a **single-node Kubernetes cluster** inside a 5 GB RAM budget.

---

## Architecture

```text
NESO Carbon Intensity API (30-min) + Open-Meteo (Northwich)
        │
        ▼
[K8s CronJob: Meltano + DuckDB]  ──▶  Bronze NDJSON (/data/bronze/)
        │                             Gold Parquet  (/data/gold/*.parquet)
        │                             DQ Assertions + Atomic Swap
        │                             pipeline-meta.json
        ▼
[Read-Only DuckDB Views (/data/views.duckdb)]
        │
        ▼
[Apache Superset + Redis Cache + Postgres] ──▶ [cloudflared Tunnel] ──▶ bi.seanstock.dev
                                                                              ▲
[seanstock.dev (Cloudflare Pages)] ──▶ /api/superset-token (Guest Token) ─────┘
```

### Key Engineering Decisions
- **Lock-free DuckDB in Kubernetes:** Rather than writing into a single `.duckdb` file held open by Gunicorn workers, the Meltano CronJob stages and atomically swaps Gold **Parquet** files on a shared RWO PVC. Superset mounts `/data` **read-only** and queries `read_parquet()` views.
- **Data Quality Gates:** Every 30-minute run asserts non-empty Gold marts, North West England (`region_id = 3`) presence, regional fuel mixes summing to 100% (±5%), and freshness lag under 4 hours before publishing.
- **Zero Open Ports:** `cloudflared` connects outbound to Cloudflare Zero Trust, routing `https://bi.seanstock.dev` directly to the internal `superset:8088` ClusterIP service.
- **Zero-Login Portfolio Embed:** `seanstock.dev` uses a Cloudflare Pages Function (`/api/superset-token`) to mint 5-minute read-only **Guest Tokens** for `@superset-ui/embedded-sdk`.

---

## 1. Run Locally (Docker Compose)

Test the entire ELT + DuckDB + Superset stack locally before deploying to K8s:

```bash
docker compose up --build
```

- **Superset UI:** [http://localhost:8088](http://localhost:8088) (`admin` / `admin`)
- **Live Telemetry Endpoint:** [http://localhost:8088/api/v1/showcase/meta](http://localhost:8088/api/v1/showcase/meta)
- All 5 Gold datasets (`gold_national_intensity`, `gold_regional_mix`, `gold_national_generation`, `gold_northwest_weather_energy`, `gold_pipeline_runs`) are auto-registered under **UK Grid Warehouse (DuckDB)**.

---

## 2. Deploy to Single-Node Kubernetes

### Step 1: Create Cloudflare Tunnel
1. In **Cloudflare Zero Trust** → **Networks** → **Tunnels**, create a tunnel (e.g. `seanstock-bi`).
2. Copy the **Tunnel Token**.
3. Add a Public Hostname: `bi.seanstock.dev` → `HTTP` → `superset.data-showcase.svc.cluster.local:8088`.

### Step 2: Create Namespace & Secrets
```bash
kubectl apply -f k8s/namespace.yaml

kubectl -n data-showcase create secret generic platform-secrets \
  --from-literal=POSTGRES_PASSWORD="$(openssl rand -hex 24)" \
  --from-literal=SUPERSET_SECRET_KEY="$(openssl rand -base64 42)" \
  --from-literal=SUPERSET_GUEST_TOKEN_SECRET="$(openssl rand -base64 42)" \
  --from-literal=SUPERSET_ADMIN_USER="admin" \
  --from-literal=SUPERSET_ADMIN_PASSWORD="<choose-strong-password>" \
  --from-literal=SUPERSET_EMBED_USER="embed_service" \
  --from-literal=SUPERSET_EMBED_PASSWORD="<choose-strong-embed-password>" \
  --from-literal=CLOUDFLARED_TUNNEL_TOKEN="<your-cloudflare-tunnel-token>"
```

### Step 3: Push Repo to GitHub (triggers multi-arch GHCR build)
Push this repo to `github.com/Sean-Stock/seanstock-data-platform` and make the two GHCR packages public (or attach an `imagePullSecret` to K8s).

### Step 4: Apply K8s Manifests & Run Initial Backfill
```bash
kubectl apply -k k8s/

# Trigger an immediate 7-day backfill run before first Superset chart creation
kubectl -n data-showcase create job --from=cronjob/meltano-uk-grid-elt meltano-initial-backfill
kubectl -n data-showcase logs -f job/meltano-initial-backfill

# Refresh Superset Gold dataset metadata once views.duckdb exists
kubectl -n data-showcase rollout restart deployment/superset
```

### Step 5: Enable Embedded Dashboard in Superset & Connect `seanstock.dev`
1. Log into `https://bi.seanstock.dev`, build & **Publish** your UK Grid dashboard.
2. Open the dashboard menu (`...`) → **Embed dashboard**, set Allowed Domains to `https://seanstock.dev`, and copy the **Embedded Dashboard UUID**.
3. In **Cloudflare Pages** → `seanstock-dev` → **Settings** → **Environment variables (Production)**, set:
   - `SUPERSET_URL` = `https://bi.seanstock.dev`
   - `SUPERSET_EMBED_USER` = `embed_service`
   - `SUPERSET_EMBED_PASSWORD` = `<same embed password from K8s secret>`
   - `SUPERSET_DASHBOARD_ID` = `<your embedded dashboard UUID>`
4. Redeploy `seanstock.dev` — the **Live Data Platform** section will automatically switch from preview mode to your live interactive Superset dashboard!
