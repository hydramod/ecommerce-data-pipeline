
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List
from urllib.parse import urljoin
import httpx

from app.core.auth import get_current_identity
from app.core.config import settings
from app.store.cart_store import get_cart, put_item, delete_item, clear_cart

router = APIRouter()

class CartItemAdd(BaseModel):
    product_id: int
    qty: int = Field(ge=1)

class CartItemUpdate(BaseModel):
    qty: int = Field(ge=0)

class CartItemRead(BaseModel):
    product_id: int
    qty: int
    unit_price_cents: int
    title: str

class CartRead(BaseModel):
    items: List[CartItemRead] = []

@router.get("/v1/cart", response_model=CartRead)
def get_my_cart(identity: dict = Depends(get_current_identity)):
    email = identity.get("sub")
    return get_cart(email)

@router.post("/v1/cart/items", response_model=CartRead, status_code=201)
def add_item(payload: CartItemAdd, identity: dict = Depends(get_current_identity)):
    email = identity.get("sub")
    # fetch product from catalog to snapshot price/title
    url = f"{settings.CATALOG_BASE}/catalog/v1/products/{payload.product_id}"
    try:
        with httpx.Client(timeout=5.0) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                raise HTTPException(status_code=404, detail="Product not found")
            p = resp.json()
    except httpx.RequestError:
        raise HTTPException(status_code=503, detail="Catalog unavailable")

    item = {
        "product_id": payload.product_id,
        "qty": payload.qty,
        "unit_price_cents": p["price_cents"],
        "title": p["title"],
    }
    put_item(email, item)
    return get_cart(email)

@router.patch("/v1/cart/items/{product_id}", response_model=CartRead)
def update_item(product_id: int, payload: CartItemUpdate, identity: dict = Depends(get_current_identity)):
    email = identity.get("sub")
    if payload.qty == 0:
        delete_item(email, product_id)
        return get_cart(email)
    # get existing (if not exists, error)
    cart = get_cart(email)
    exists = next((i for i in cart["items"] if i["product_id"] == product_id), None)
    if not exists:
        raise HTTPException(status_code=404, detail="Item not in cart")
    exists["qty"] = payload.qty
    put_item(email, exists)
    return get_cart(email)

@router.delete("/v1/cart/items/{product_id}", response_model=CartRead)
def remove_item(product_id: int, identity: dict = Depends(get_current_identity)):
    email = identity.get("sub")
    delete_item(email, product_id)
    return get_cart(email)

@router.post("/v1/cart/clear", response_model=CartRead)
def clear(identity: dict = Depends(get_current_identity)):
    email = identity.get("sub")
    clear_cart(email)
    return get_cart(email)
