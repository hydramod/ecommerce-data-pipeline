# Event Contracts

This stack uses **Kafka** for async, decoupled workflows. All events are **addition-only** and follow a stable envelope.

## Topics

* `order.events` — order lifecycle (e.g., `order.created`)
* `payment.events` — payment lifecycle (e.g., `payment.succeeded`)
* `shipping.events` — shipping lifecycle (e.g., `shipping.ready`, `shipping.dispatched`)

**Partition key**: `order_id` (as string) to co-locate related messages.

## Common envelope

```json
{
  "type": "payment.succeeded",
  "order_id": 123,
  "user_email": "cust@example.com",
  "timestamp": "2025-08-25T20:12:34Z",
  "data": { /* event-specific fields */ },
  "version": 1
}
```

* `type`: dot-namespaced event type.
* `order_id`: required on all commerce events.
* `user_email`: included when relevant (created/paid/ready).
* `timestamp`: ISO8601 UTC.
* `data`: event-specific payload (see below).
* `version`: schema version (increment when adding fields).

> **Idempotency**: Consumers must handle duplicates. Use upserts keyed by `order_id`/`shipment_id` and enforce legal state transitions.

---

## Event Types

### `order.events`

#### `order.created`

Emitted by **Order** after successful checkout and inventory reservation.

```json
{
  "type": "order.created",
  "order_id": 123,
  "user_email": "cust@example.com",
  "timestamp": "2025-08-25T20:00:00Z",
  "data": {
    "amount_cents": 25998,
    "currency": "USD",
    "items": [
      { "product_id": 1, "qty": 2, "unit_price_cents": 12999 }
    ]
  },
  "version": 1
}
```

(Optionally in future: `order.cancelled`, `order.refunded`.)

---

### `payment.events`

#### `payment.succeeded`

Emitted by **Payment** (mock) when a payment goes through.

```json
{
  "type": "payment.succeeded",
  "order_id": 123,
  "user_email": "cust@example.com",
  "timestamp": "2025-08-25T20:01:00Z",
  "data": {
    "amount_cents": 25998,
    "currency": "USD",
    "provider": "mock",
    "payment_id": "pm_abc123"
  },
  "version": 1
}
```

(Planned: `payment.failed` with `reason`, `code`.)

**Consumers**

* **Order**: set `status: PAID`.
* **Shipping**: advance shipment to `READY_TO_SHIP` and emit `shipping.ready`.
* **Notifications**: email the customer.

---

### `shipping.events`

#### `shipping.ready`

Emitted by **Shipping** when payment clears and shipment becomes `READY_TO_SHIP`.

```json
{
  "type": "shipping.ready",
  "order_id": 123,
  "user_email": "cust@example.com",
  "timestamp": "2025-08-25T20:02:00Z",
  "data": {
    "shipment_id": 1
  },
  "version": 1
}
```

#### `shipping.dispatched`

Emitted by **Shipping** after dispatch API is called.

```json
{
  "type": "shipping.dispatched",
  "order_id": 123,
  "user_email": "cust@example.com",
  "timestamp": "2025-08-25T20:03:00Z",
  "data": {
    "shipment_id": 1,
    "carrier": "demo-carrier",
    "tracking_number": "TRK-0001"
  },
  "version": 1
}
```

(Planned: `shipping.delivered`, `shipping.cancelled`.)

**Consumers**

* **Notifications**: send status emails.
* (Optional future) analytics, audit loggers, etc.

---

## Error Handling & Delivery Semantics

* **At-least-once** delivery: services should be tolerant to duplicates.
* **State transitions** must be legal (e.g., cannot dispatch from `PENDING_PAYMENT`).
* **Poison messages**: log and skip with metrics; a DLQ topic can be introduced later if needed.
* **Ordering**: per-order ordering is maintained by keying with `order_id`. Avoid cross-order coupling.

---

## Local debugging tips

* **Check topics/consumers**: use Kafkacat/`kcat` (if available) against `kafka:9092`.
* **Verify emails**: open MailHog UI at [http://localhost:8025](http://localhost:8025).
* **Follow the flow**: `scripts/run_demo.ps1` prints each step, logs response bodies, polls shipping, and lists recent emails.

---

