# Public & Internal APIs

**Base URLs via gateway (Traefik):**

```
/auth, /catalog, /cart, /order, /payment, /shipping, /notifications
```

Each service also exposes `/health` and `/v1/_info`.

> ⚠️ Endpoints marked **(internal)** require `X-Internal-Key: <SVC_INTERNAL_KEY>` and are intended for service-to-service calls.

---

## Auth

### Register

```
POST /auth/register
```

Body:

```json
{ "email": "user@example.com", "password": "P@ssw0rd!", "role": "admin|customer?" }
```

* `role` is optional (defaults to `customer`); only admins should create admins.

### Login

```
POST /auth/login
```

Body:

```json
{ "email": "user@example.com", "password": "P@ssw0rd!" }
```

Response:

```json
{
  "access_token": "<jwt>",
  "refresh_token": "<jwt>",
  "token_type": "bearer"
}
```

### Me

```
GET /users/me
Authorization: Bearer <access_token>
```

---

## Catalog

### Categories

```
POST /catalog/v1/categories/         (admin)
GET  /catalog/v1/categories/         (list)
GET  /catalog/v1/categories/{id}     (read)
```

Create body:

```json
{ "name": "Shoes" }
```

### Products

```
POST /catalog/v1/products/           (admin)
GET  /catalog/v1/products/{id}
PATCH/PUT /catalog/v1/products/{id}  (admin)  [if implemented]
```

Create body (example):

```json
{
  "title": "Air Zoom",
  "description": "Runner",
  "price_cents": 12999,
  "currency": "USD",
  "sku": "SKU-001",
  "category_id": 1,
  "active": true
}
```

### Inventory

```
POST /catalog/v1/inventory/restock   (internal + admin)
POST /catalog/v1/inventory/reserve   (internal)
```

**restock** body:

```json
{ "items": [ {"product_id": 1, "qty": 50} ] }
```

**reserve** body (used by Order checkout):

```json
{ "items": [ {"product_id": 1, "qty": 2}, ... ] }
```

---

## Cart

```
GET    /cart/v1/cart
POST   /cart/v1/cart/items
PATCH  /cart/v1/cart/items/{product_id}
DELETE /cart/v1/cart/items/{product_id}
POST   /cart/v1/cart/clear
```

**Add item** body:

```json
{ "product_id": 1, "qty": 2 }
```

* The cart service snapshots `title` and `unit_price_cents` from Catalog at add-time.

---

## Order

### Checkout

```
POST /order/v1/orders/checkout
Authorization: Bearer <access_token>
```

Body:

```json
{
  "address_line1": "1 Demo Street",
  "address_line2": "",
  "city": "Dublin",
  "country": "IE",
  "postcode": "D01XYZ"
}
```

Behavior:

* Reads user cart from Redis.
* Calls Catalog **reserve** (internal).
* Creates `order` with status `CREATED`.
* Creates a **shipment draft** in Shipping (`PENDING_PAYMENT`).
* Emits `order.created`.

Response:

```json
{
  "order_id": 123,
  "status": "CREATED",
  "total_cents": 25998,
  "currency": "USD"
}
```

### Get order

```
GET /order/v1/orders/{order_id}
```

Response (example):

```json
{
  "id": 123,
  "status": "PAID",
  "total_cents": 25998,
  "currency": "USD",
  "items": [ { "product_id": 1, "qty": 2, "unit_price_cents": 12999 } ]
}
```

---

## Payment

> Mock implementation for the demo.

```
POST /payment/v1/payments/mock-succeed
```

Body:

```json
{ "order_id": 123, "amount_cents": 25998, "currency": "USD" }
```

Behavior:

* Publishes `payment.succeeded` to Kafka.

(Optionally you can add a `mock-fail` in the future to emit `payment.failed`.)

---

## Shipping

```
POST /shipping/v1/shipments
GET  /shipping/v1/shipments?order_id=<id>
POST /shipping/v1/shipments/{shipment_id}/dispatch
```

**Create** body (normally called by Order during checkout):

```json
{
  "order_id": 123,
  "user_email": "cust@example.com",
  "address_line1": "1 Demo Street",
  "address_line2": "",
  "city": "Dublin",
  "country": "IE",
  "postcode": "D01XYZ"
}
```

* New shipments start as `PENDING_PAYMENT`.
* On `payment.succeeded`, Shipping updates to `READY_TO_SHIP` and emits `shipping.ready`.
* **Dispatch** moves `READY_TO_SHIP` → `DISPATCHED` and can emit `shipping.dispatched`.

---

## Notifications

* HTTP surface is just `/health` and `/v1/_info`.
* The worker consumes from Kafka and sends emails to MailHog.

---

## Health & Info (all services)

* `GET /<svc>/health` → `{ "status": "ok" }`
* `GET /<svc>/v1/_info` → `{ "service": "<name>", "version": "<semver>" }`

---
