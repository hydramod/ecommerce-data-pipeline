# File: catalog/app/api/inventory.py
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select
from app.api.deps import get_db
from app.db.models import Inventory
from app.core.config import settings
import jwt
from jwt import PyJWTError, ExpiredSignatureError, InvalidTokenError
import logging

logger = logging.getLogger(__name__)
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
    if x_internal_key and x_internal_key == settings.SVC_INTERNAL_KEY:
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
            options={"verify_aud": False}
        )
    except (PyJWTError, ExpiredSignatureError, InvalidTokenError):
        raise HTTPException(status_code=401, detail="Invalid token")

    if payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin required")

    return True

@router.post("/v1/inventory/reserve")
def reserve(req: ItemsReq, db: Session = Depends(get_db), _=Depends(admin_or_internal)):
    try:
        with db.begin():
            for it in req.items:
                # Use row locking to prevent race conditions
                inv = db.execute(
                    select(Inventory)
                    .where(Inventory.product_id == it.product_id)
                    .with_for_update()
                ).scalar_one_or_none()
                
                if not inv:
                    raise HTTPException(status_code=404, detail=f"Inventory missing for product_id {it.product_id}")
                
                available = (inv.in_stock or 0) - (inv.reserved or 0)
                if available < it.qty:
                    raise HTTPException(status_code=409, detail=f"Insufficient stock for product_id {it.product_id}")
                
                inv.reserved = (inv.reserved or 0) + it.qty
                db.add(inv)
        
        return {"status": "reserved"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in reserve operation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/v1/inventory/commit")
def commit(req: ItemsReq, db: Session = Depends(get_db), _=Depends(admin_or_internal)):
    try:
        with db.begin():
            for it in req.items:
                inv = db.execute(
                    select(Inventory)
                    .where(Inventory.product_id == it.product_id)
                    .with_for_update()
                ).scalar_one_or_none()
                
                if not inv:
                    raise HTTPException(status_code=404, detail=f"Inventory missing for product_id {it.product_id}")
                
                inv.in_stock = (inv.in_stock or 0) - it.qty
                inv.reserved = max(0, (inv.reserved or 0) - it.qty)
                db.add(inv)
        
        return {"status": "committed"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in commit operation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/v1/inventory/restock")
def restock(req: ItemsReq, db: Session = Depends(get_db), _=Depends(admin_or_internal)):
    try:
        with db.begin():
            for it in req.items:
                inv = db.execute(
                    select(Inventory)
                    .where(Inventory.product_id == it.product_id)
                    .with_for_update()
                ).scalar_one_or_none()
                
                if not inv:
                    inv = Inventory(product_id=it.product_id, in_stock=0, reserved=0)
                
                inv.in_stock = (inv.in_stock or 0) + max(0, it.qty)
                db.add(inv)
        
        return {"status": "restocked"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in restock operation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
