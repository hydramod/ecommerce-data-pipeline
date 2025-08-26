# Payment Service

Mock payment processor for the e-commerce stack. Exposes simple endpoints to create a (fake) payment intent and to signal a successful payment, which is published to Kafka for the Order service to consume.&#x20;

---

## Responsibilities

* **Create payment intents** (mock) — returns a fake `payment_id` and `client_secret`.&#x20;
* **Emit payment events** — publishes `payment.succeeded` to Kafka topic `payment.events`.

No database is used by this service.

---

## Environment

Configured via env vars (defaults shown):&#x20;

* `KAFKA_BOOTSTRAP` — Kafka bootstrap server, e.g. `kafka:9092`

---

## HTTP API

Base prefix: `/payment` (Traefik routes this service under that path). Health + info endpoints are also exposed.&#x20;

### Health

* `GET /payment/health` → `{ "status": "ok" }`
* `GET /health` → `{ "status": "ok" }` (container liveness)
* `GET /v1/_info` → `{"service":"payment","version": "<semver>"}`&#x20;

### Create Intent (mock)

`POST /payment/v1/payments/create-intent`

**Body**

```json
{
  "order_id": 123,
  "amount_cents": 25998,
  "currency": "USD"
}
```

**Response**

```json
{
  "payment_id": "pay_123",
  "client_secret": "secret_123"
}
```

Returns deterministic mock values (`pay_<order_id>`, `secret_<order_id>`). No external PSP is contacted.&#x20;

### Mock Succeed

`POST /payment/v1/payments/mock-succeed`

**Body**

```json
{
  "order_id": 123,
  "amount_cents": 25998,
  "currency": "USD"
}
```

**Behavior**

* Publishes to Kafka topic `payment.events` with payload:

  ```json
  {
    "type": "payment.succeeded",
    "order_id": 123,
    "amount_cents": 25998,
    "currency": "USD"
  }
  ```

  Then responds with `{"status":"ok"}`.

---

## Events

* **Topic**: `payment.events`
* **Producer**: `KafkaProducer` (JSON value serializer, string key serializer), configured from `KAFKA_BOOTSTRAP`. Messages are flushed after send.&#x20;

---

## Running

### Docker (recommended)

This service is included in the repo’s `deploy/docker-compose.yaml`. From the repo root:

```bash
docker compose -f deploy/docker-compose.yaml --env-file deploy/.env up -d payment
```

### Local dev

```bash
uvicorn app.main:app --reload --port 8000
```

The app prints all registered routes on startup for debugging.&#x20;

---

## Examples

```bash
# Create a payment intent
curl -s http://localhost/payment/v1/payments/create-intent \
  -H "Content-Type: application/json" \
  -d '{"order_id":123,"amount_cents":25998,"currency":"USD"}'

# Signal a successful payment (emits payment.succeeded)
curl -s http://localhost/payment/v1/payments/mock-succeed \
  -H "Content-Type: application/json" \
  -d '{"order_id":123,"amount_cents":25998,"currency":"USD"}'
```

---

**Service entrypoint**: `app.main:app` (FastAPI), router mounted at `/payment`.&#x20;
