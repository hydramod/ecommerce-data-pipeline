# services/catalog/app/api/inventory.py
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy.orm import Session
from app.api.deps import get_db
from app.db.models import Inventory, Product
from app.core.config import settings
import jwt

router = APIRouter()

class Item(BaseModel):
    product_id: int
    qty: int

class ItemsReq(BaseModel):
    items: List[Item]

def admin_or_internal(
    x_internal_key: Optional[str] = Header(default=None, alias="X-Internal-Key"),
    auth: Optional[str] = Header(default=None, alias="Authorization"),
):
    # 1) allow trusted internal calls
    if x_internal_key and x_internal_key == (getattr(settings, "SVC_INTERNAL_KEY", "") or ""):
        return True

    # 2) otherwise require admin JWT
    if not auth or not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    token = auth.split(" ", 1)[1] 
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET,
            algorithms=[settings.JWT_ALGORITHM],
        )
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    if payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin required")

    return True

@router.post("/v1/inventory/reserve")
def reserve(req: ItemsReq, db: Session = Depends(get_db),
            _=Depends(admin_or_internal)):
    for it in req.items:
        inv = db.get(Inventory, it.product_id)
        if not inv:
            raise HTTPException(status_code=404, detail=f"Inventory missing for product_id {it.product_id}")
        if (inv.in_stock or 0) - (inv.reserved or 0) < it.qty:
            raise HTTPException(status_code=409, detail=f"Insufficient stock for product_id {it.product_id}")
        inv.reserved = (inv.reserved or 0) + it.qty
        db.add(inv)
    db.commit()
    return {"status": "reserved"}

@router.post("/v1/inventory/commit")
def commit(req: ItemsReq, db: Session = Depends(get_db),
           _=Depends(admin_or_internal)):
    for it in req.items:
        inv = db.get(Inventory, it.product_id)
        if not inv:
            raise HTTPException(status_code=404, detail=f"Inventory missing for product_id {it.product_id}")
        inv.in_stock = (inv.in_stock or 0) - it.qty
        inv.reserved = max(0, (inv.reserved or 0) - it.qty)
        db.add(inv)
    db.commit()
    return {"status": "committed"}

@router.post("/v1/inventory/restock")
def restock(req: ItemsReq, db: Session = Depends(get_db),
            _=Depends(admin_or_internal)):  # <- changed from require_admin
    for it in req.items:
        inv = db.get(Inventory, it.product_id)
        if not inv:
            inv = Inventory(product_id=it.product_id, in_stock=0, reserved=0)
        inv.in_stock = (inv.in_stock or 0) + max(0, it.qty)
        db.add(inv)
    db.commit()
    return {"status": "restocked"}
