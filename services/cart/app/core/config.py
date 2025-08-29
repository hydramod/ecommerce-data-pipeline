# File: cart/app/core/config.py
from pydantic import BaseModel
import os

class Settings(BaseModel):
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    CATALOG_BASE: str = os.getenv("CATALOG_BASE", "http://catalog:8000")
    #CATALOG_BASE_EXTERNAL: str = os.getenv("CATALOG_BASE_EXTERNAL", "http://gateway/catalog")
    JWT_SECRET: str = os.getenv("JWT_SECRET") or os.getenv("JWT_SECRET", "")
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")

# Validate required settings
if not os.getenv('JWT_SECRET'):
    import warnings
    warnings.warn("JWT_SECRET environment variable is not set. Using empty string which is INSECURE for production.")

settings = Settings()
