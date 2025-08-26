Here’s an accurate, service-scoped `services/auth/README.md` based on the files you shared (routes, schemas, alembic env, users route, etc.).

---

# Auth Service

FastAPI microservice for **user registration**, **login**, and **JWT** issuance (access + refresh), with refresh-token revocation. Exposed via the gateway under **`/auth`** and user listing under **`/users`**.

* DB: Postgres (SQLAlchemy + Alembic)
* JWT: HS256 (PyJWT)
* Passwords: hashed (e.g., bcrypt via utility helpers)
* Alembic version table: **`alembic_version_auth`** (see `alembic/env.py`)

> This service is part of the larger e-commerce stack but can be run independently for development.

---

## Endpoints (Gateway paths)

### Health

* `GET /auth/health` → `{"status":"ok"}`

### Auth (v1)

* `POST /auth/register`
  Request body (`RegisterPayload`):

  ```json
  {
    "email": "user@example.com",
    "password": "P@ssw0rd!",
    "role": "customer"   // optional; defaults to "customer"
  }
  ```

  * **201 Created**: `{"status":"created"}`
  * **409 Conflict**: `{"detail":"Email already registered"}`

* `POST /auth/login`
  Request body (`LoginPayload`):

  ```json
  { "email": "user@example.com", "password": "P@ssw0rd!" }
  ```

  * **200 OK** (`TokenPair`):

    ```json
    {
      "access_token": "<jwt>",
      "refresh_token": "<jwt>",
      "token_type": "bearer"
    }
    ```
  * **401 Unauthorized** on invalid credentials.

* `POST /auth/refresh`
  Request body:

  ```json
  { "refresh_token": "<jwt>" }
  ```

  * **200 OK** (`TokenPair`): issues a brand-new access token and a **rotated** refresh token.
  * **401 Unauthorized** if the token is invalid, not a `refresh` token, expired, or revoked.

* `POST /auth/logout`
  Request body:

  ```json
  { "refresh_token": "<jwt>" }
  ```

  * **200 OK**: revokes the refresh token (by `jti`) if present.
  * **401 Unauthorized** if token is invalid/not a refresh token.

### Users (v1)

* `GET /users/` → list users (id, email, role)

  ```json
  [
    { "id": 1, "email": "admin@example.com", "role": "admin" },
    { "id": 2, "email": "cust@example.com", "role": "customer" }
  ]
  ```

  > The route is defined in `app/api/v1/routes_users.py`. Adjust auth/roles as needed.

---

## Data Model (overview)

While the models live under `app/db/models.py` and migrations under `alembic/versions/`, the auth service expects:

* **User**

  * `id` (PK)
  * `email` (unique)
  * `role` (`admin` / `customer` / etc.)
  * `password_hash`
  * timestamps (if present)

* **RefreshToken**

  * `id` (PK), `user_id` (FK → users)
  * `jti` (JWT ID), **`token_hash`** (sha256 of refresh token)
  * `expires_at`, `revoked` (bool), `created_at`

The Alembic env sets:

```py
VERSION_TABLE = "alembic_version_auth"
```

---

## JWT & Security

* **Algorithms**: `HS256`
* **Access token**: short-lived, claim `type: "access"`
* **Refresh token**: long-lived, claim `type: "refresh"`, includes a `jti`
* **Rotate on refresh**: a new refresh token is issued; previous one typically revoked (see routes).
* Common claims:

  * `sub`: user email
  * `role`: user role
  * `type`: `"access"` / `"refresh"`
  * `jti`: unique ID on refresh tokens
  * `exp`: expiry

Use the access token on protected endpoints:

```
Authorization: Bearer <access_token>
```

---

## Configuration

Environment variables (with sane defaults in Docker):

| Variable                       | Default                                                      | Purpose                |
| ------------------------------ | ------------------------------------------------------------ | ---------------------- |
| `POSTGRES_DSN`                 | `postgresql+psycopg://postgres:postgres@postgres:5432/appdb` | SQLAlchemy DSN         |
| `JWT_SECRET`                   | `devsecret`                                                  | HMAC secret for JWT    |
| `JWT_ALGORITHM`                | `HS256`                                                      | JWT algorithm          |
| `ACCESS_TOKEN_EXPIRES_SECONDS` | `900` (example default)                                      | Access token lifetime  |
| `REFRESH_TOKEN_EXPIRES_DAYS`   | `30` (example default)                                       | Refresh token lifetime |

> Exact defaults are defined in `app/core/config.py`. Adjust in compose `.env` for production.

---

## Running (Docker)

This service is already wired in the root `deploy/docker-compose.yaml`. To bring up **auth + postgres** only:

```bash
cp deploy/.env.example deploy/.env
docker compose -f deploy/docker-compose.yaml --env-file deploy/.env up -d --build auth postgres
```

Run migrations:

```bash
python scripts/seed.py
# or
powershell -ExecutionPolicy Bypass -File .\scripts\seed.ps1
```

Health check:

```bash
curl -s http://localhost/auth/health
```

---

## Running (Local Uvicorn)

```bash
# venv
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows PowerShell
# .\.venv\Scripts\Activate.ps1

# install editable
pip install -e services/auth

# local env (DSN points to your local Postgres)
export POSTGRES_DSN="postgresql+psycopg://postgres:postgres@localhost:5432/appdb"
export JWT_SECRET="devsecret"
export JWT_ALGORITHM="HS256"

# migrate
cd services/auth
alembic upgrade head

# run
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

When running standalone (without Traefik), hit `http://localhost:8000`.

---

## Curl examples

```bash
# Register
curl -sX POST http://localhost/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"admin@example.com","password":"P@ssw0rd!","role":"admin"}'

# Login
curl -sX POST http://localhost/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"admin@example.com","password":"P@ssw0rd!"}'

# Refresh
curl -sX POST http://localhost/auth/refresh \
  -H 'Content-Type: application/json' \
  -d '{"refresh_token":"<jwt>"}'

# Logout (revoke refresh)
curl -sX POST http://localhost/auth/logout \
  -H 'Content-Type: application/json' \
  -d '{"refresh_token":"<jwt>"}'

# List users
curl -s http://localhost/users/
```

---

## Project Layout

```
services/auth/
├─ alembic/
│  ├─ versions/                # migration scripts (e.g., 20250823142355_init_auth.py)
│  └─ env.py                   # VERSION_TABLE = "alembic_version_auth"
├─ app/
│  ├─ api/
│  │  ├─ deps.py               # get_db, etc.
│  │  └─ v1/
│  │     ├─ routes_auth.py     # /register, /login, /refresh, /logout
│  │     ├─ routes_users.py    # /users/
│  │     └─ schemas.py         # RegisterPayload, LoginPayload, TokenPair...
│  ├─ core/config.py           # settings (env-based)
│  ├─ db/
│  │  ├─ models.py             # User, RefreshToken (and related)
│  │  └─ session.py            # engine, SessionLocal, Base
│  ├─ security/utils.py        # hashing, JWT create/decode, token SHA256, now_utc
│  ├─ main.py                  # FastAPI app
│  └─ version.py               # version info
├─ Dockerfile
├─ pyproject.toml
└─ tests/test_health.py
```

---

## Notes

* Duplicate register attempts return **409** with a clear message.
* Refresh & logout validate token **type** (`"refresh"`) and **revoke** by `jti`.
* Alembic version table is **service-scoped**: `alembic_version_auth`.

---
