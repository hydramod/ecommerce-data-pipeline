from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.core.auth import get_current_identity
from app.core.config import settings
from app.store.cart_store import get_cart, put_item, delete_item, clear_cart
import logging

logger = logging.getLogger(__name__)
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

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(httpx.RequestError)
)
def fetch_product(product_id: int):
    url = f"{settings.CATALOG_BASE}/catalog/v1/products/{product_id}"
    try:
        with httpx.Client(timeout=5.0) as client:
            resp = client.get(url)
            if resp.status_code == 404:
                raise HTTPException(status_code=404, detail="Product not found")
            resp.raise_for_status()
            return resp.json()
    except httpx.RequestError as e:
        logger.error(f"Catalog service request failed: {e}")
        raise HTTPException(status_code=503, detail="Catalog service unavailable")
    except httpx.HTTPStatusError as e:
        logger.error(f"Catalog service returned error: {e}")
        raise HTTPException(status_code=e.response.status_code, detail="Catalog service error")

@router.get("/v1/cart", response_model=CartRead)
def get_my_cart(identity: dict = Depends(get_current_identity)):
    email = identity.get("sub")
    return get_cart(email)

@router.post("/v1/cart/items", response_model=CartRead, status_code=201)
def add_item(payload: CartItemAdd, identity: dict = Depends(get_current_identity)):
    email = identity.get("sub")
    
    product = fetch_product(payload.product_id)
    
    item = {
        "product_id": payload.product_id,
        "qty": payload.qty,
        "unit_price_cents": product["price_cents"],
        "title": product["title"],
    }
    put_item(email, item)
    return get_cart(email)

@router.patch("/v1/cart/items/{product_id}", response_model=CartRead)
def update_item(product_id: int, payload: CartItemUpdate, identity: dict = Depends(get_current_identity)):
    email = identity.get("sub")
    
    cart = get_cart(email)
    existing_item = next((i for i in cart["items"] if i["product_id"] == product_id), None)
    if not existing_item:
        raise HTTPException(status_code=404, detail="Item not in cart")
    
    if payload.qty == 0:
        delete_item(email, product_id)
    else:
        updated_item = {**existing_item, "qty": payload.qty}
        put_item(email, updated_item)
    
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
