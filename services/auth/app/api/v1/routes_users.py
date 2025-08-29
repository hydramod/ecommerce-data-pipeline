from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.api.deps import get_db
from app.db.models import User

router = APIRouter()

@router.get("/", response_model=list[dict])
def list_users(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
):
    users = db.query(User).offset(skip).limit(limit).all()
    return [{"id": u.id, "email": u.email, "role": u.role} for u in users]
