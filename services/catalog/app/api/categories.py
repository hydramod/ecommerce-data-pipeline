from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.api.deps import get_db
from app.db.models import Category
from app.schemas import CategoryCreate, CategoryRead

router = APIRouter()

@router.get('/', response_model=List[CategoryRead])
def list_categories(db: Session = Depends(get_db)):
    return db.query(Category).order_by(Category.name).all()

@router.post('/', response_model=CategoryRead, status_code=201)
def create_category(payload: CategoryCreate, db: Session = Depends(get_db)):
    if db.query(Category).filter(Category.name == payload.name).first():
        raise HTTPException(status_code=409, detail='Category already exists')
    obj = Category(name=payload.name)
    db.add(obj); db.commit(); db.refresh(obj)
    return obj
