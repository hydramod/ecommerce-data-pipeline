# Cart Service

FastAPI microservice that manages a user’s shopping cart in **Redis**.

* Stores each user’s cart under `cart:<email>` as a Redis **hash** of `product_id -> JSON`.
* Snapshots **price** and **title** from the Catalog service at add time.
* Requires a valid **JWT access token** (issued by Auth) for all cart APIs.

Exposed via the gateway under the **`/cart`** prefix.

---

## Endpoints (gateway paths)

### Health & Info

* `GET /cart/health` → `{"status":"ok"}`
* `GET /cart/v1/_info` → service/version info
  (There’s also a service-local `GET /health`, which is not routed via the gateway.)

### Cart

All cart endpoints require:
`Authorization: Bearer <access_token>`

* `GET /cart/v1/cart`
  Returns the current cart for the authenticated user.

  ```json
  {
    "items": [
      { "product_id": 1, "qty": 2, "unit_price_cents": 12999, "title": "Air Zoom" }
    ]
  }
  ```

* `POST /cart/v1/cart/items`
  Add or replace an item. The service calls Catalog to snapshot `price_cents` and `title`.

  ```json
  { "product_id": 1, "qty": 2 }
  ```

  Responses:

  * **201** with the full cart
  * **404** if product not found in Catalog
  * **503** if Catalog is unavailable

* `PATCH /cart/v1/cart/items/{product_id}`
  Update quantity; **qty = 0** removes the item.

  ```json
  { "qty": 3 }
  ```

  Responses:

  * **200** with the full cart
  * **404** if item not currently in the cart

* `DELETE /cart/v1/cart/items/{product_id}`
  Remove the item (no-op if it isn’t present).
  **200** with the full cart.

* `POST /cart/v1/cart/clear`
  Clear the entire cart.
  **200** with an empty cart.

---

## Request/Response Models

* **CartItemAdd**

  ```json
  { "product_id": 123, "qty": 1 }
  ```
* **CartItemUpdate**

  ```json
  { "qty": 0 }
  ```
* **CartItemRead**

  ```json
  { "product_id": 123, "qty": 1, "unit_price_cents": 1999, "title": "Example" }
  ```
* **CartRead**

  ```json
  { "items": [CartItemRead, ...] }
  ```

---

## How it works

* The **email** from the JWT (`sub` claim) is used as the cart key: `cart:<email>`.
* On **add**, the service fetches Catalog:

  ```
  GET {CATALOG_BASE}/catalog/v1/products/{product_id}
  ```

  and stores a snapshot (`unit_price_cents`, `title`).
* Data is stored in Redis as a hash of `product_id -> JSON` for quick read/modify.

---

## Configuration

Environment variables (defaults come from `app/core/config.py`):

| Variable        | Default                | Purpose                             |
| --------------- | ---------------------- | ----------------------------------- |
| `REDIS_URL`     | `redis://redis:6379/0` | Redis connection string             |
| `CATALOG_BASE`  | `http://catalog:8000`  | Base URL to reach Catalog in Docker |
| `JWT_SECRET`    | `devsecret`            | HMAC secret for verifying JWT       |
| `JWT_ALGORITHM` | `HS256`                | JWT algorithm                       |

> In Docker, `catalog` and `redis` are service hostnames. Through the Traefik gateway you’ll call `/cart/...`.

---

## Project Layout

```
services/cart/
├─ app/
│  ├─ api/routes.py          # /v1/cart, /v1/cart/items, etc.
│  ├─ core/auth.py           # Bearer auth, jwt decode/validate (type=access)
│  ├─ core/config.py         # Settings from env
│  ├─ store/cart_store.py    # Redis helpers
│  ├─ main.py                # FastAPI app + router include (prefix '/cart')
│  └─ version.py
├─ tests/test_health.py
├─ Dockerfile
├─ pyproject.toml
└─ README.md
```

---

## Run with Docker

This service is wired into `deploy/docker-compose.yaml` already. To run just Cart + Redis via compose:

```bash
cp deploy/.env.example deploy/.env
docker compose -f deploy/docker-compose.yaml --env-file deploy/.env up -d --build cart redis
```

Health:

```bash
curl -s http://localhost/cart/health
```

---

## Local (without Docker)

```bash
# venv
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows PowerShell
# .\.venv\Scripts\Activate.ps1

# install editable
pip install -e services/cart

# set env (point to your local Redis and Catalog)
export REDIS_URL="redis://localhost:6379/0"
export CATALOG_BASE="http://localhost:8000"
export JWT_SECRET="devsecret"
export JWT_ALGORITHM="HS256"

# run
cd services/cart
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

When running standalone (no gateway), call it directly at `http://localhost:8000/cart/...`.

---

## Curl examples

Assuming you already have a valid access token in `$TOKEN`:

```bash
# Show cart
curl -s http://localhost/cart/v1/cart \
  -H "Authorization: Bearer $TOKEN"

# Add item
curl -sX POST http://localhost/cart/v1/cart/items \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"product_id":1,"qty":2}'

# Update qty
curl -sX PATCH http://localhost/cart/v1/cart/items/1 \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"qty":3}'

# Remove item
curl -sX DELETE http://localhost/cart/v1/cart/items/1 \
  -H "Authorization: Bearer $TOKEN"

# Clear cart
curl -sX POST http://localhost/cart/v1/cart/clear \
  -H "Authorization: Bearer $TOKEN"
```

---

## Troubleshooting

* **401 Not authenticated / Invalid token**: Ensure you’re sending an **access** token (`type=access`), not a refresh token.
* **404 Product not found** during add: Catalog doesn’t know that `product_id`. Create it first or check the ID.
* **503 Catalog unavailable**: The cart container cannot reach Catalog. In Docker, verify `CATALOG_BASE` and that the Catalog service is healthy.
* **404 Item not in cart** on `PATCH`: You can only update items already present; otherwise, `POST` first.

---
