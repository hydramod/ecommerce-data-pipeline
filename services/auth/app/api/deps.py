from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.security.utils import decode_token
from app.db.models import User

security = HTTPBearer(auto_error=False)

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

def get_current_user(creds: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)) -> User:
    if not creds: raise HTTPException(status_code=401, detail='Not authenticated')
    try:
        payload = decode_token(creds.credentials)
    except Exception:
        raise HTTPException(status_code=401, detail='Invalid token')
    if payload.get('type') != 'access':
        raise HTTPException(status_code=401, detail='Invalid access token')
    user = db.query(User).filter(User.email == payload.get('sub')).first()
    if not user: raise HTTPException(status_code=401, detail='User not found')
    return user

def require_role(required: str):
    def _checker(user: User = Depends(get_current_user)):
        if user.role != required:
            raise HTTPException(status_code=403, detail='Forbidden')
        return user
    return _checker
