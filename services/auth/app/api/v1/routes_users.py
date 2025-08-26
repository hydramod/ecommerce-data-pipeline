from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.api.deps import get_db
from app.db.models import User

router = APIRouter()

@router.get("/", response_model=list[dict])
def list_users(db: Session = Depends(get_db)):
    return [{"id": u.id, "email": u.email, "role": u.role} for u in db.query(User).all()]
