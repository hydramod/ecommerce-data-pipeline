
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import jwt, os
from app.core.config import settings

security = HTTPBearer(auto_error=False)

def get_current_identity(creds: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    if not creds:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = jwt.decode(creds.credentials, settings.JWT_SECRET, algorithms=[os.getenv("JWT_ALGORITHM","HS256")])
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")
    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid access token")
    return payload

def require_admin(identity: dict = Depends(get_current_identity)):
    if identity.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    return identity
