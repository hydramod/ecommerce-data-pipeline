from fastapi import APIRouter, Depends, HTTPException, Header
from typing import List, Optional
from pydantic import BaseModel, EmailStr
from redis import Redis
import httpx, os
from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.db import models
from app.core.config import settings
from app.kafka.producer import send
import jwt, json

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_identity_dep(authorization: str | None = Header(default=None, alias="Authorization")) -> dict:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    token = authorization.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")
    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid access token")
    return payload

def redis_client() -> Redis:
    return Redis.from_url(settings.REDIS_URL, decode_responses=True)

# --- New body model for shipping details ---
class ShippingAddress(BaseModel):
    address_line1: str
    address_line2: str | None = ""
    city: str
    country: str   # "IE", "US", etc
    postcode: str

class CheckoutResponse(BaseModel):
    order_id: int
    status: str
    total_cents: int
    currency: str

@router.post("/v1/orders/checkout", response_model=CheckoutResponse)
def checkout(payload: ShippingAddress, identity: dict = Depends(get_identity_dep), db: Session = Depends(get_db)):
    email = identity.get("sub")
    # Read cart from Redis
    r = redis_client()
    key = f"cart:{email}"
    raw = r.hgetall(key)  # {product_id: item_json}
    if not raw:
        raise HTTPException(status_code=400, detail="Cart is empty")

    items = []
    total = 0
    for _, v in raw.items():
        it = json.loads(v)
        items.append(it)
        total += int(it["qty"]) * int(it["unit_price_cents"])

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
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
    except httpx.RequestError:
        raise HTTPException(status_code=503, detail="Catalog unavailable")

    # Create order in DB
    order = models.Order(user_email=email, status="CREATED", total_cents=total, currency="USD")
    db.add(order); db.commit(); db.refresh(order)
    for it in items:
        oi = models.OrderItem(
            order_id=order.id,
            product_id=it["product_id"],
            qty=it["qty"],
            unit_price_cents=it["unit_price_cents"],
            title_snapshot=it["title"],
        )
        db.add(oi)
    db.commit()

    # Create shipment in Shipping service (draft: PENDING_PAYMENT)
    try:
        with httpx.Client(timeout=5.0) as client:
            sresp = client.post(
                f"{settings.SHIPPING_BASE}/shipping/v1/shipments",
                json={
                    "order_id": order.id,
                    "user_email": email,
                    "address_line1": payload.address_line1,
                    "address_line2": payload.address_line2 or "",
                    "city": payload.city,
                    "country": payload.country,
                    "postcode": payload.postcode,
                },
            )
            if sresp.status_code >= 400:
                raise HTTPException(status_code=502, detail="Shipping create failed")
    except httpx.RequestError:
        raise HTTPException(status_code=503, detail="Shipping unavailable")

    # Emit event
    send(
        topic="order.events",
        key=str(order.id),
        value={
            "type": "order.created",
            "order_id": order.id,
            "user_email": email,
            "amount_cents": total,
            "items": [
                {
                    "product_id": it["product_id"],
                    "qty": it["qty"],
                    "unit_price_cents": it["unit_price_cents"],
                }
                for it in items
            ],
        },
    )

    return CheckoutResponse(order_id=order.id, status=order.status, total_cents=order.total_cents, currency=order.currency)

from sqlalchemy import select

@router.get("/v1/orders/{order_id}")
def get_order(order_id: int, db: Session = Depends(get_db)):
    obj = db.get(models.Order, order_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Order not found")
    return {
        "id": obj.id,
        "status": obj.status,
        "total_cents": obj.total_cents,
        "currency": obj.currency,
        "items": [
            {"product_id": it.product_id, "qty": it.qty, "unit_price_cents": it.unit_price_cents}
            for it in obj.items
        ],
    }
