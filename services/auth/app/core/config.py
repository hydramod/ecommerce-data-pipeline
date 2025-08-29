# File: auth/app/core/config.py
from pydantic import BaseModel
import os

class Settings(BaseModel):
    POSTGRES_DSN: str = os.getenv('POSTGRES_DSN', 'postgresql+psycopg://postgres:postgres@postgres:5432/appdb')
    JWT_SECRET: str = os.getenv('JWT_SECRET') or os.getenv('JWT_SECRET', '')  # No default in production
    JWT_ALGORITHM: str = os.getenv('JWT_ALGORITHM', 'HS256')
    ACCESS_TOKEN_EXPIRES_SECONDS: int = int(os.getenv('ACCESS_TOKEN_EXPIRES_SECONDS', '900'))
    REFRESH_TOKEN_EXPIRES_DAYS: int = int(os.getenv('REFRESH_TOKEN_EXPIRES_DAYS', '30'))

# Validate required settings on import
if not os.getenv('JWT_SECRET'):
    import warnings
    warnings.warn("JWT_SECRET environment variable is not set. Using empty string which is INSECURE for production.")

settings = Settings()
