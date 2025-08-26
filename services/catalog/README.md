# Catalog Service

Product catalog + inventory for the e-commerce stack. Exposes product/category CRUD, image uploads to S3/MinIO, and inventory ops used by other services.

* Framework: FastAPI
* DB: PostgreSQL (SQLAlchemy + Alembic)
* Object storage: S3/MinIO

## Endpoints

Health & info

* `GET /catalog/health` – liveness via gateway
* `GET /health` – liveness (service port)
* `GET /v1/_info` – service + version

### Categories

* `GET /catalog/v1/categories/` – list all.
* `POST /catalog/v1/categories/` – create (409 on duplicate name).&#x20;

### Products

* `GET /catalog/v1/products/` – list with filters:

  * `q` (title ilike), `category_id`, `active`, `limit`, `offset`.
* `GET /catalog/v1/products/{id}` – fetch one (404 if missing).
* `POST /catalog/v1/products/` – create product (409 on duplicate `sku`).
  Also auto-creates an `inventory` row with `in_stock=0,reserved=0`.
* `PATCH /catalog/v1/products/{id}` – partial update.
* `POST /catalog/v1/products/{id}/images` – upload image (multipart `file`) → stored on S3/MinIO; URL returned on the product payload.&#x20;

### Inventory (internal/admin)

All require either:

* `X-Internal-Key: <SVC_INTERNAL_KEY>` **or**
* `Authorization: Bearer <admin JWT>`

Endpoints:

* `POST /catalog/v1/inventory/reserve` – reserve stock (409 if insufficient).
* `POST /catalog/v1/inventory/commit` – decrement `in_stock`, release `reserved`.
* `POST /catalog/v1/inventory/restock` – add to `in_stock`.&#x20;

> Note: Only **inventory** routes enforce auth in-code. Category/Product routes are open in this service; you can secure them at the gateway or add a dependency as needed.&#x20;

## Data Model (SQLAlchemy)

* `categories(id, name UNIQUE)`
* `products(id, title, description, price_cents, currency, sku UNIQUE, category_id, active)`
* `product_images(id, product_id, object_key, url)`
* `inventory(product_id PK, in_stock, reserved)`
  (See `app/db/models.py` and Alembic migration.)

## Configuration

Env vars (see `app/core/config.py`):&#x20;

```
POSTGRES_DSN=postgresql+psycopg://postgres:postgres@postgres:5432/appdb
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=admin
S3_SECRET_KEY=adminadmin
S3_BUCKET=catalog-media
S3_SECURE=false            # "true" to use https for URLs
JWT_SECRET=devsecret
JWT_ALGORITHM=HS256
SVC_INTERNAL_KEY=devkey    # used by inventory endpoints
```

## Run (Docker)

This service is wired behind Traefik with `PathPrefix(/catalog)`. From repo root:

```bash
docker compose -f deploy/docker-compose.yaml --env-file deploy/.env up -d --build catalog
```

Health check via gateway:

```bash
curl http://localhost/catalog/health
```

## DB Migrations

Alembic version table: `alembic_version_catalog`.&#x20;

```bash
# inside services/catalog/
alembic upgrade head
```

## Local Dev (uvicorn)

```bash
cd services/catalog
uvicorn app.main:app --reload --port 8000
```

## Quick Examples

Create category (409 if exists):

```bash
curl -X POST http://localhost/catalog/v1/categories/ \
  -H 'Content-Type: application/json' \
  -d '{"name":"Shoes"}'
```

Create product:

```bash
curl -X POST http://localhost/catalog/v1/products/ \
  -H 'Content-Type: application/json' \
  -d '{"title":"Air Zoom","description":"Runner","price_cents":12999,"currency":"USD","sku":"SKU-001","category_id":1,"active":true}'
```

Restock (internal key or admin JWT):

```bash
curl -X POST http://localhost/catalog/v1/inventory/restock \
  -H 'X-Internal-Key: devkey' \
  -H 'Content-Type: application/json' \
  -d '{"items":[{"product_id":1,"qty":50}]}'
```

Upload image:

```bash
curl -X POST http://localhost/catalog/v1/products/1/images \
  -F "file=@/path/to/image.jpg"
```

## Tests

```bash
cd services/catalog
pytest -q
```

---
