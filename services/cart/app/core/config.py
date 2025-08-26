
from pydantic import BaseModel
import os

class Settings(BaseModel):
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    CATALOG_BASE: str = os.getenv("CATALOG_BASE", "http://catalog:8000")
    JWT_SECRET: str = os.getenv("JWT_SECRET", "devsecret")
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")

settings = Settings()
