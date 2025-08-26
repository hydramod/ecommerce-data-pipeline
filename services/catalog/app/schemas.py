from pydantic import BaseModel, Field
from typing import Optional, List

class CategoryBase(BaseModel):
    name: str
class CategoryCreate(CategoryBase): pass
class CategoryRead(CategoryBase):
    id: int
    class Config: from_attributes = True
class ProductImageRead(BaseModel):
    id: int
    url: str
    object_key: str
    class Config: from_attributes = True
class InventoryRead(BaseModel):
    in_stock: int
    reserved: int
    class Config: from_attributes = True
class ProductBase(BaseModel):
    title: str
    description: Optional[str] = ''
    price_cents: int = Field(ge=0)
    currency: str = 'USD'
    sku: str
    category_id: Optional[int] = None
    active: bool = True
class ProductCreate(ProductBase): pass
class ProductUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    price_cents: Optional[int] = Field(default=None, ge=0)
    currency: Optional[str] = None
    category_id: Optional[int] = None
    active: Optional[bool] = None
class ProductRead(ProductBase):
    id: int
    images: List[ProductImageRead] = []
    inventory: Optional[InventoryRead] = None
    class Config: from_attributes = True
