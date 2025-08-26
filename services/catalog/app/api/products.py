from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select
from app.api.deps import get_db
from app.core.auth import require_admin
from app.db import models
from app.schemas import ProductCreate, ProductUpdate, ProductRead
from app.services.storage import upload_bytes

router = APIRouter()

@router.get('/', response_model=List[ProductRead])
def list_products(db: Session = Depends(get_db), q: Optional[str] = None, limit: int = 50, offset: int = 0, category_id: Optional[int] = None, active: Optional[bool] = None):
    stmt = select(models.Product)
    if q:
        q_like = f"%{q.lower()}%"
        stmt = stmt.where(models.Product.title.ilike(q_like))
    if category_id is not None: stmt = stmt.where(models.Product.category_id == category_id)
    if active is not None: stmt = stmt.where(models.Product.active == active)
    stmt = stmt.offset(offset).limit(limit)
    return db.execute(stmt).scalars().unique().all()

@router.get('/{product_id}', response_model=ProductRead)
def get_product(product_id: int, db: Session = Depends(get_db)):
    obj = db.get(models.Product, product_id)
    if not obj: raise HTTPException(status_code=404, detail='Product not found')
    return obj

@router.post('/', response_model=ProductRead, status_code=201)
def create_product(payload: ProductCreate, db: Session = Depends(get_db)):
    if db.query(models.Product).filter(models.Product.sku == payload.sku).first():
        raise HTTPException(status_code=409, detail='SKU already exists')
    obj = models.Product(**payload.model_dump())
    db.add(obj); db.add(models.Inventory(product=obj, in_stock=0, reserved=0)); db.commit(); db.refresh(obj)
    return obj

@router.patch('/{product_id}', response_model=ProductRead)
def update_product(product_id: int, payload: ProductUpdate, db: Session = Depends(get_db)):
    obj = db.get(models.Product, product_id)
    if not obj: raise HTTPException(status_code=404, detail='Product not found')
    for k, v in payload.model_dump(exclude_unset=True).items(): setattr(obj, k, v)
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

@router.post('/{product_id}/images', response_model=ProductRead)
async def upload_product_image(product_id: int, file: UploadFile = File(...), db: Session = Depends(get_db)):
    obj = db.get(models.Product, product_id)
    if not obj: raise HTTPException(status_code=404, detail='Product not found')
    content = await file.read(); ext = '.' + file.filename.rsplit('.',1)[-1].lower() if '.' in file.filename else ''
    key, url = upload_bytes(content, file.content_type or 'application/octet-stream', ext=ext)
    img = models.ProductImage(product=obj, object_key=key, url=url)
    db.add(img); db.commit(); db.refresh(obj)
    return obj
