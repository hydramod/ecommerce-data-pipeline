from fastapi import APIRouter, Depends, HTTPException, Header, Request
from typing import List
from pydantic import BaseModel, constr, validator
from redis import Redis, ConnectionPool
import httpx
import json
import jwt
from sqlalchemy.orm import Session, selectinload
from app.db.session import get_db
from app.db import models
from app.core.config import settings
from app.kafka.producer import publish_order_event
from app.core.limiting import limiter
from slowapi.errors import RateLimitExceeded
from fastapi.responses import JSONResponse
import logging
from datetime import datetime
from uuid import uuid4

logger = logging.getLogger(__name__)
router = APIRouter()

# Redis connection pooling
_redis_pool = None

def get_redis_pool():
    global _redis_pool
    if _redis_pool is None:
        _redis_pool = ConnectionPool.from_url(
            settings.REDIS_URL, decode_responses=True, max_connections=10
        )
    return _redis_pool

def redis_client() -> Redis:
    return Redis(connection_pool=get_redis_pool())

def check_redis_health():
    """Check Redis connection health"""
    try:
        redis = redis_client()
        redis.ping()
        return True
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        return False

def get_identity_dep(authorization: str | None = Header(default=None, alias="Authorization")) -> dict:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not settings.JWT_SECRET:
        raise HTTPException(status_code=500, detail="JWT configuration missing")

    token = authorization.split(" ", 1)[1]
    try:
        jwt_options = {}
        if settings.JWT_ISSUER:
            jwt_options["verify_iss"] = True
        if settings.JWT_AUDIENCE:
            jwt_options["verify_aud"] = True

        payload = jwt.decode(
            token,
            settings.JWT_SECRET,
            algorithms=[settings.JWT_ALGORITHM],
            options=jwt_options,
            issuer=settings.JWT_ISSUER or None,
            audience=settings.JWT_AUDIENCE or None,
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {e}")
        raise HTTPException(status_code=401, detail="Invalid token")
    except Exception as e:
        logger.error(f"Token validation error: {e}")
        raise HTTPException(status_code=401, detail="Invalid token")

    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid access token")
    return payload

# --- Models ---
class ShippingAddress(BaseModel):
    address_line1: constr(max_length=100)
    address_line2: constr(max_length=100) = ""
    city: constr(max_length=50)
    country: constr(min_length=2, max_length=2)
    postcode: constr(max_length=20)

    @validator("country")
    def country_must_be_upper_case(cls, v):
        return v.upper()

class CheckoutResponse(BaseModel):
    order_id: int
    status: str
    total_cents: int
    currency: str

class OrderItemResponse(BaseModel):
    product_id: int
    qty: int
    unit_price_cents: int
    title_snapshot: str

class OrderResponse(BaseModel):
    id: int
    status: str
    total_cents: int
    currency: str
    items: List[OrderItemResponse]

# --- Routes ---
@router.post("/v1/orders/checkout", response_model=CheckoutResponse)
@limiter.limit("5/minute")
def checkout(
    request: Request,
    payload: ShippingAddress,
    identity: dict = Depends(get_identity_dep),
    db: Session = Depends(get_db),
):
    """
    - Reads cart from Redis
    - Reserves inventory (Catalog)
    - Persists Order + Items
    - Publishes 'order.created' event (no Shipping HTTP calls)
    """
    email = identity.get("sub")
    if not email:
        raise HTTPException(status_code=401, detail="Invalid identity")

    # Read cart from Redis
    r = redis_client()
    raw = r.hgetall(f"cart:{email}")
    if not raw:
        raise HTTPException(status_code=400, detail="Cart is empty")

    items = []
    total = 0
    for _, v in raw.items():
        try:
            it = json.loads(v)
            items.append(it)
            total += int(it["qty"]) * int(it["unit_price_cents"])
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Invalid cart item format: {e}")
            raise HTTPException(status_code=400, detail="Invalid cart data")

    # Reserve inventory via Catalog internal API
    reserve_req = {"items": [{"product_id": it["product_id"], "qty": it["qty"]} for it in items]}
    try:
        with httpx.Client(timeout=5.0) as client:
            resp = client.post(
                f"{settings.CATALOG_BASE}/catalog/v1/inventory/reserve",
                json=reserve_req,
                headers={"X-Internal-Key": settings.SVC_INTERNAL_KEY},
            )
            if resp.status_code != 200:
                logger.error(f"Inventory reserve failed: {resp.text}")
                raise HTTPException(status_code=resp.status_code, detail="Inventory reservation failed")
    except httpx.RequestError as e:
        logger.error(f"Catalog service unavailable: {e}")
        raise HTTPException(status_code=503, detail="Catalog service unavailable")

    # Create order in DB
    try:
        order = models.Order(user_email=email, total_cents=total, currency="USD")
        db.add(order)
        db.flush()  # assign id

        for it in items:
            db.add(
                models.OrderItem(
                    order_id=order.id,
                    product_id=it["product_id"],
                    qty=it["qty"],
                    unit_price_cents=it["unit_price_cents"],
                    title_snapshot=it["title"],
                )
            )

        db.commit()      # ensure persistence before emitting event
        db.refresh(order)
    except Exception as e:
        db.rollback()
        logger.error(f"Order creation failed: {e}")
        raise HTTPException(status_code=500, detail="Order creation failed")

    # Publish order.created (pure event-driven from here)
    try:
        trace_id = request.headers.get("X-Request-ID") or str(uuid4())
        order_items_for_event = [
            {"product_id": it["product_id"], "qty": it["qty"], "price": it["unit_price_cents"] / 100.0}
            for it in items
        ]

        evt = {
            "event_type": "order.created",
            "event_version": "1.0",
            "trace_id": trace_id,
            "order_id": str(order.id),
            "user_id": email,
            "items": order_items_for_event,
            "currency": order.currency,
            "total_amount": order.total_cents / 100.0,
            "status": order.status,
            "event_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "shipping": {
                "address_line1": payload.address_line1,
                "address_line2": payload.address_line2 or "",
                "city": payload.city,
                "country": payload.country,
                "postcode": payload.postcode,
            },
        }

        publish_order_event(evt)
        
    except Exception as e:
        # For demo we don't fail checkout if Kafka hiccups
        logger.error(f"Failed to publish order event: {e}")

    # Return summary
    return CheckoutResponse(
        order_id=order.id,
        status=order.status,
        total_cents=order.total_cents,
        currency=order.currency,
    )

@router.get("/v1/orders/{order_id}", response_model=OrderResponse)
def get_order(order_id: int, db: Session = Depends(get_db)):
    order = (
        db.query(models.Order)
        .options(selectinload(models.Order.items))
        .filter(models.Order.id == order_id)
        .first()
    )
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    return OrderResponse(
        id=order.id,
        status=order.status,
        total_cents=order.total_cents,
        currency=order.currency,
        items=[
            OrderItemResponse(
                product_id=it.product_id,
                qty=it.qty,
                unit_price_cents=it.unit_price_cents,
                title_snapshot=it.title_snapshot,
            )
            for it in order.items
        ],
    )
