# Notifications Service

Consumes domain events from Kafka and sends transactional emails (via SMTP/MailHog). Minimal public API is provided for test emails. No database required.

* Framework: FastAPI
* Messaging: Kafka (consumer)
* Mail: SMTP (MailHog in local dev)

---

## What it does

* Subscribes to three topics: `order.events`, `payment.events`, `shipping.events`.&#x20;
* On startup, a background thread starts the Kafka consumer and processes messages continuously.&#x20;
* Sends plaintext emails using SMTP; defaults target to the local MailHog container.

### Event types handled

The consumer inspects the `type` field and uses the following keys from the payload to build emails:&#x20;

* **`order.created`** → caches `order_id → user_email` and emails “Order received” (uses `order_id`, `user_email`, `amount_cents`).
* **`payment.succeeded`** → emails “Payment received” (uses `order_id`; resolves `user_email` from event or cache).
* **`shipping.ready`** → emails “Order ready to ship” (uses `order_id` + `user_email` lookup).
* **`shipping.dispatched`** → emails “Order dispatched” with `tracking_number` if present.

> The service keeps a simple in-memory map `{order_id: user_email}` learned from `order.created` for later events that omit `user_email`.&#x20;

---

## API

All routes are mounted under the Traefik path prefix `/notifications`.

* `GET /notifications/health` – liveness via gateway.&#x20;
* `GET /health` – liveness on the service port.&#x20;
* `POST /notifications/v1/test-email` – sends a test email (demo-only). Body:

  ```json
  { "to": "you@example.com", "subject": "Hi", "body": "Hello!" }
  ```



---

## Configuration

Environment variables (defaults shown):&#x20;

```env
KAFKA_BOOTSTRAP=kafka:9092
TOPIC_ORDER_EVENTS=order.events
TOPIC_PAYMENT_EVENTS=payment.events
TOPIC_SHIPPING_EVENTS=shipping.events

SMTP_HOST=mailhog
SMTP_PORT=1025
FROM_EMAIL=no-reply@example.local
```

Kafka consumer config highlights:

* `group_id = "notifications-service"`, `auto_offset_reset = "earliest"`, JSON value deserializer.&#x20;

---

## Run (Docker)

This service is expected to run inside the full docker-compose stack behind Traefik:

```bash
docker compose -f deploy/docker-compose.yaml --env-file deploy/.env up -d notifications
```

Health check:

```bash
curl http://localhost/notifications/health
```

Test email (will appear in MailHog UI at [http://localhost:8025](http://localhost:8025)):

```bash
curl -X POST http://localhost/notifications/notifications/v1/test-email \
  -H 'Content-Type: application/json' \
  -d '{"to":"test@example.com","subject":"Test","body":"Hello from notifications!"}'
```

> The app starts the Kafka consumer automatically on FastAPI startup and stops it on shutdown.&#x20;

---

## Local development

Run with auto-reload:

```bash
cd services/notifications
uvicorn app.main:app --reload --port 8000
```

This will still try to connect to Kafka and SMTP per your env; for isolated local testing you can use the `test-email` HTTP endpoint without Kafka.&#x20;

---

## Expected event payloads (examples)

```json
// order.events
{ "type":"order.created","order_id":123,"user_email":"cust@example.com","amount_cents":25998 }

// payment.events
{ "type":"payment.succeeded","order_id":123,"user_email":"cust@example.com" }

// shipping.events (ready)
{ "type":"shipping.ready","order_id":123,"user_email":"cust@example.com" }

// shipping.events (dispatched)
{ "type":"shipping.dispatched","order_id":123,"tracking_number":"ZX999999IE","user_email":"cust@example.com" }
```

> `user_email` is optional on non-order events; the service will fall back to its cache if available.&#x20;

---

## Security notes

* The `/notifications/v1/test-email` route is **unauthenticated** and intended purely for local/demo usage—disable or protect it for any real environment.&#x20;
* Messages are trusted inputs; ensure only internal producers can write to the Kafka topics (broker ACLs / network policy).

---

## Project structure

```
app/
  api/routes.py          # FastAPI routes (test email)
  core/config.py         # env-driven settings
  kafka/consumer.py      # Kafka loop + email handlers
  main.py                # startup/shutdown hooks + health
```

---

## Troubleshooting

* **No emails appear** → check MailHog is running and `SMTP_HOST/PORT` are correct.&#x20;
* **No events consumed** → confirm `KAFKA_BOOTSTRAP`, topic names, and that producers are emitting JSON with a `type` field matching the handled cases.&#x20;

---

## Tests

From the service directory:

```bash
pytest -q
```

(Health test included.)
