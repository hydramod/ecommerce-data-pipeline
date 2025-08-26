# E-Commerce Microservices (Python)

A small, dockerized microservices stack that demonstrates an e-commerce flow end-to-end:

* **Auth** – JWT auth (FastAPI + Postgres)
* **Catalog** – products, categories, inventory (FastAPI + Postgres + MinIO S3)
* **Cart** – user cart in Redis (FastAPI + Redis)
* **Order** – creates orders, emits events (FastAPI + Postgres + Kafka)
* **Payment** – mock payment API that emits events (FastAPI + Kafka)
* **Shipping** – shipment lifecycle (FastAPI + Postgres + Kafka)
* **Notifications** – email via MailHog, reacts to events (FastAPI + Kafka + MailHog)
* **Gateway** – Traefik routing (`/auth`, `/catalog`, `/cart`, `/order`, `/payment`, `/shipping`, `/notifications`)
* **Monitoring** – Prometheus + Grafana (+ optional infra exporters)

> ⚠️ Demo stack. Secrets and security are intentionally simple.

---

## Contents

* [Quick start](#quick-start)
* [Environment](#environment)
* [Demo script](#demo-script)
* [Monitoring (Prometheus + Grafana)](#monitoring-prometheus--grafana)

  * [Optional infra exporters](#optional-infra-exporters)
* [Services & endpoints](#services--endpoints)
* [Events](#events)
* [Local development](#local-development)
* [Troubleshooting](#troubleshooting)

---

## Quick start

Bring everything up with Docker, run DB migrations, then execute the demo.

### 1) Start the stack

```bash
# from repo root
cp deploy/.env.example deploy/.env
docker compose -f deploy/docker-compose.yaml --env-file deploy/.env up -d --build
```

This starts: Postgres, Redis, Kafka+ZooKeeper, MinIO, MailHog, Traefik, all services, **Prometheus** and **Grafana**.

Useful UIs:

* Traefik dashboard — [http://localhost:8080](http://localhost:8080)
* MailHog — [http://localhost:8025](http://localhost:8025)
* Prometheus — [http://localhost:9090](http://localhost:9090)
* Grafana — [http://localhost:3000](http://localhost:3000)  (default login: `admin` / `admin`)

### 2) Run database migrations

```bash
# cross-platform Python helper
python scripts/seed.py
```

> Reads `deploy/.env`, rewrites DSN host to `localhost`, and runs `alembic upgrade head` for services with migrations (auth, catalog, order, shipping).

### 3) Run the end-to-end demo

PowerShell (Windows):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_demo.ps1
```

Python (any platform):

```bash
python scripts/run_demo.py
```

What it does:

1. Admin/customer register/login
2. Admin creates category/product, restocks inventory
3. Customer adds to cart & **checkout** (creates shipment draft)
4. **Payment** mock success → event
5. **Shipping** becomes `READY_TO_SHIP` and dispatches
6. Prints recent **emails** in MailHog

---

## Environment

Copy and tweak `deploy/.env.example`:

```dotenv
POSTGRES_DSN=postgresql+psycopg://postgres:postgres@postgres:5432/appdb
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=appdb

REDIS_URL=redis://redis:6379/0

KAFKA_BOOTSTRAP=kafka:9092

S3_ENDPOINT=http://minio:9000
S3_BUCKET=media
S3_ACCESS_KEY=admin
S3_SECRET_KEY=adminadmin

JWT_SECRET=devsecret
JWT_ALGORITHM=HS256

SVC_INTERNAL_KEY=devkey
```

Notes:

* Service base URLs default to Docker DNS names (e.g. `http://catalog:8000`), so you usually don’t need to set them.
* The demo reads MailHog at `http://localhost:8025`.

---

## Monitoring (Prometheus + Grafana)

**Files:** `deploy/monitoring/prometheus.yaml` (mounted into the Prometheus container).

The compose file already runs Prometheus (port `9090`) and Grafana (port `3000`) with scrape jobs for every service:

```yaml
# example (full config in deploy/monitoring/prometheus.yaml)
global:
  scrape_interval: 15s
scrape_configs:
  - job_name: 'auth'
    metrics_path: /auth/metrics
    static_configs: [{ targets: ['auth:8000'] }]
  # ... cart, catalog, order, payment, shipping, notifications
  # ... postgres-exporter, redis-exporter, kafka-exporter (optional)
```

### How to use

1. **Open Grafana:** [http://localhost:3000](http://localhost:3000) (login `admin` / `admin` on first run).
2. **Add a Prometheus data source:**

   * *URL:* `http://prometheus:9090` (because Grafana talks to Prometheus over the Docker network)
   * Leave the rest default and **Save & test**.
3. **Build a dashboard** and add panels using PromQL. Some handy starters:

   * Service up/down (per job):
     `max by (job) (up{job=~"auth|cart|catalog|order|payment|shipping|notifications"})`
   * Scrape health (per job):
     `sum by (job) (increase(scrape_samples_scraped[5m]))`
   * Avg scrape duration (ms):
     `avg by (job) (rate(scrape_duration_seconds_sum[5m]) / rate(scrape_duration_seconds_count[5m])) * 1000`
   * 5-minute availability (%):
     `avg_over_time(up{job=~"auth|cart|catalog|order|payment|shipping|notifications"}[5m]) * 100`

> If you instrument your apps with request metrics, you can also chart things like request rate/error rate/latency (e.g., `http_requests_total`, `request_duration_seconds_*`, etc.).

### Optional infra exporters

Add infra metrics by keeping these services enabled in `deploy/docker-compose.yaml` (already included in our compose):

* **Postgres** — `prometheuscommunity/postgres-exporter` (targets `postgres-exporter:9187`)
* **Redis** — `bitnami/redis-exporter` (targets `redis-exporter:9121`)
* **Kafka** — `danielqsj/kafka-exporter` (targets `kafka-exporter:9308`)

> If Docker Hub pulling is blocked on your network, pre-pull images manually or mirror them to an internal registry.

---

## Services & endpoints

**Common**

* Health: `GET /<service>/health`
* Info (if present): `GET /<service>/v1/_info`

**Catalog (`/catalog`)**

* `POST /v1/categories/` – create category (Admin JWT)
* `POST /v1/products/` – create product (Admin JWT)
* `GET  /v1/products/{id}` – fetch product
* `POST /v1/inventory/restock` – restock (Admin JWT + `X-Internal-Key`)

**Cart (`/cart`)**

* `POST /v1/cart/items` – add item (Customer JWT) `{ "product_id": int, "qty": int }`
* `PATCH/DELETE /v1/cart/items/{product_id}` – update/remove
* `POST /v1/cart/clear` – clear

**Order (`/order`)**

* `POST /v1/orders/checkout` – create order & shipment draft (body = shipping address)
* `GET  /v1/orders/{order_id}` – fetch order

**Payment (`/payment`)**

* `POST /v1/payments/mock-succeed` – simulate success `{ "order_id": int, "amount_cents": int, "currency": "USD" }`

**Shipping (`/shipping`)**

* `GET  /v1/shipments?order_id={id}`
* `POST /v1/shipments/{id}/dispatch`
* Statuses: `PENDING_PAYMENT → READY_TO_SHIP → DISPATCHED`

**Notifications (`/notifications`)**

* Consumes events and emails via MailHog (see [http://localhost:8025](http://localhost:8025))

---

## Events

Kafka bootstrap: `kafka:9092` (inside Docker).

* **Topic `order.events`**

  * `order.created` — Order on checkout
* **Topic `payment.events`**

  * `payment.succeeded` — Payment mock
* **Topic `shipping.events`**

  * `shipping.ready`, `shipping.dispatched` — Shipping state changes

**Consumers**

* Shipping ← `order.created`, `payment.succeeded`
* Notifications ← `order.created`, `payment.succeeded`, `shipping.dispatched`

---

## Local development

### Scripts (cross-platform)

* `scripts/setup.py` – create venv and install all services (editable)
* `scripts/seed.py` – run Alembic migrations locally
* `scripts/rebuild.py` – clean rebuild all containers/images
* `scripts/gen-service-reqs.py` – (dev) generate per-service `requirements.txt` via pipreqs
* Demo: `scripts/run_demo.ps1` (PowerShell) or `scripts/run_demo.py` (Python)

### Virtual env

```bash
python -m venv .venv
# Linux/macOS:
source .venv/bin/activate
# Windows PowerShell:
# .\.venv\Scripts\Activate.ps1

pip install -r requirements-dev.txt
# Example: run a service locally
cd services/auth && uvicorn app.main:app --reload
```

---

## Troubleshooting

**409 Conflict “already exists”**
Expected on re-runs; scripts print a friendly message.

**Shipping 500 / “relation shipments does not exist”**
Run migrations: `python scripts/seed.py`.

**Cart 404 on add**
Cart can’t reach Catalog. Check `CATALOG_BASE` env in the *Cart* container and that Catalog is healthy.

**Kafka connection errors**
Ensure `zookeeper` and `kafka` are running; services depend on `kafka` in compose.

**Prometheus won’t start: “mount type mismatch”**
Make sure the file exists at `deploy/monitoring/prometheus.yaml` and the compose mount path matches the filename **exactly**.

**Reset everything**

```bash
docker compose -f deploy/docker-compose.yaml --env-file deploy/.env down -v
docker compose -f deploy/docker-compose.yaml --env-file deploy/.env up -d --build
python scripts/seed.py
```

