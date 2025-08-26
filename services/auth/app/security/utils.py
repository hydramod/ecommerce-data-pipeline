from passlib.context import CryptContext
from datetime import datetime, timedelta
import jwt, uuid, hashlib
from typing import Tuple
from app.core.config import settings

pwd_ctx = CryptContext(schemes=['bcrypt'], deprecated='auto')

def hash_password(p: str) -> str: return pwd_ctx.hash(p)

def verify_password(p: str, h: str) -> bool: return pwd_ctx.verify(p, h)

def now_utc() -> datetime: return datetime.utcnow()

def generate_jti() -> str: return uuid.uuid4().hex

def token_sha256(t: str) -> str: return hashlib.sha256(t.encode('utf-8')).hexdigest()

def create_access_token(sub: str, role: str) -> Tuple[str, datetime]:
    exp = now_utc() + timedelta(seconds=settings.ACCESS_TOKEN_EXPIRES_SECONDS)
    payload = {'sub': sub, 'role': role, 'exp': exp, 'type': 'access'}
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM), exp

def create_refresh_token(sub: str):
    exp = now_utc() + timedelta(days=settings.REFRESH_TOKEN_EXPIRES_DAYS)
    jti = generate_jti()
    payload = {'sub': sub, 'jti': jti, 'exp': exp, 'type': 'refresh'}
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM), jti, exp

def decode_token(token: str):
    return jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
