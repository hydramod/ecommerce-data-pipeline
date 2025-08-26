# Order Service

Order management for the e-commerce stack. Creates orders from the customer’s cart, reserves and commits inventory in **Catalog**, coordinates shipment creation in **Shipping**, and reacts to **Payment** events to advance order status.&#x20;

---

## Responsibilities

* **Checkout flow**: reads the user’s cart from Redis, reserves stock in Catalog, creates an Order + OrderItems in Postgres, creates a draft Shipment in Shipping, and emits an `order.created` Kafka event.&#x20;
* **Payment reaction**: consumes `payment.succeeded` events, commits inventory in Catalog and marks the Order as `PAID`.&#x20;

---

## Environment

The service is configured via env vars (defaults shown):&#x20;

* `POSTGRES_DSN` – e.g. `postgresql+psycopg://postgres:postgres@postgres:5432/appdb`
* `REDIS_URL` – e.g. `redis://redis:6379/0`
* `KAFKA_BOOTSTRAP` – e.g. `kafka:9092`
* `CATALOG_BASE` – e.g. `http://catalog:8000`
* `SHIPPING_BASE` – e.g. `http://shipping:8000`
* `SVC_INTERNAL_KEY` – shared internal key for Catalog “reserve/commit” endpoints
* `JWT_SECRET`, `JWT_ALGORITHM` – verify access tokens on checkout

---

## Data model (Postgres)

* `orders (id, user_email, status, total_cents, currency, created_at, updated_at)`
* `order_items (id, order_id, product_id, qty, unit_price_cents, title_snapshot)`
  Defined with SQLAlchemy; see `app/db/models.py`.&#x20;

Engine/session are created from `POSTGRES_DSN`.&#x20;

Alembic migration lives under `alembic/versions/…`. Run `alembic upgrade head` to create tables.

---

## HTTP API

Base prefix: `/order` (Traefik routes this service to that prefix). Core routes are included and health endpoints are exposed at startup.&#x20;

### Health

* `GET /order/health` → `{ "status": "ok" }`&#x20;
* `GET /health` → `{ "status": "ok" }` (container liveness)&#x20;
* `GET /v1/_info` → service name & version&#x20;

### Create order (checkout)

`POST /order/v1/orders/checkout`

**Auth**: Bearer access token (JWT).
**Body**:

```json
{
  "address_line1": "1 Demo Street",
  "address_line2": "",
  "city": "Dublin",
  "country": "IE",
  "postcode": "D01XYZ"
}
```

**Behavior**:

1. Read cart from `redis://…` (`cart:{email}`), total the amount.
2. Reserve inventory in Catalog: `POST {CATALOG_BASE}/catalog/v1/inventory/reserve` with `X-Internal-Key`.
3. Create `Order` + `OrderItem`s in DB.
4. Create Shipment (draft, `PENDING_PAYMENT`): `POST {SHIPPING_BASE}/shipping/v1/shipments`.
5. Emit Kafka `order.created` to `order.events`.&#x20;

**Response**:

```json
{
  "order_id": 123,
  "status": "CREATED",
  "total_cents": 25998,
  "currency": "USD"
}
```

### Get order

`GET /order/v1/orders/{order_id}` → order header + items.&#x20;

---

## Events

### Publishes

* **Topic**: `order.events`
  **Type**: `order.created`
  **Payload** (example):

  ```json
  {
    "type": "order.created",
    "order_id": 123,
    "user_email": "cust@example.com",
    "amount_cents": 25998,
    "items": [{"product_id":1,"qty":2,"unit_price_cents":12999}]
  }
  ```

  Produced with a Kafka producer configured from `KAFKA_BOOTSTRAP`.&#x20;

### Consumes

* **Topic**: `payment.events`
  **On** `payment.succeeded`:

  * POST Catalog `inventory/commit` with the order’s items (using `X-Internal-Key`)
  * Update Order status → `PAID` and commit.&#x20;

---

## Inter-service calls

* **Catalog**

  * `POST /catalog/v1/inventory/reserve` (checkout step)&#x20;
  * `POST /catalog/v1/inventory/commit` (after `payment.succeeded`)&#x20;
    Include header `X-Internal-Key: {SVC_INTERNAL_KEY}`.

* **Shipping**

  * `POST /shipping/v1/shipments` to create a shipment draft tied to the order.&#x20;

---

## Running

### Docker (recommended)

This service is part of the top-level `deploy/docker-compose.yaml`. Build & run from the repo root:

```bash
docker compose -f deploy/docker-compose.yaml --env-file deploy/.env up -d order
```

### Local dev (uvicorn)

```bash
# With env vars set for DB/Kafka/Redis/etc.
uvicorn app.main:app --reload --port 8000
```

The app starts a Kafka consumer on startup to process `payment.events`. Health routes are printed to the logs at boot.&#x20;

---

## Testing

A minimal health test is included under `tests/test_health.py`. Run with your preferred test runner (e.g., `pytest -q`).

---

## Notes & Gotchas

* Checkout will fail with 5xx if Catalog/Shipping aren’t reachable; ensure `CATALOG_BASE` and `SHIPPING_BASE` are correct inside Docker.&#x20;
* Inventory changes are a **two-phase** process: reserve at checkout, commit after payment. The order status transitions `CREATED → PAID` exclusively via the `payment.succeeded` event.
* The service expects valid **access** JWTs; other token types are rejected.&#x20;

---

## API Examples

```bash
# Checkout (Bearer token required)
curl -X POST http://localhost/order/v1/orders/checkout \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
        "address_line1":"1 Demo Street",
        "address_line2":"",
        "city":"Dublin",
        "country":"IE",
        "postcode":"D01XYZ"
      }'

# Get order
curl http://localhost/order/v1/orders/123
```

---

**Service entrypoint:** `app.main:app` (FastAPI), includes router prefix `/order` and starts the Kafka consumer on startup.&#x20;
