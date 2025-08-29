from pydantic import BaseModel
import os

class Settings(BaseModel):
    POSTGRES_DSN: str = os.getenv('POSTGRES_DSN', 'postgresql+psycopg://postgres:postgres@postgres:5432/appdb')
    REDIS_URL: str = os.getenv('REDIS_URL', 'redis://redis:6379/0')
    KAFKA_BOOTSTRAP: str = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
    CATALOG_BASE: str = os.getenv('CATALOG_BASE', 'http://catalog:8000')
    SHIPPING_BASE: str = os.getenv('SHIPPING_BASE', 'http://shipping:8000')
    JWT_SECRET: str = os.getenv('JWT_SECRET') or ''
    JWT_ALGORITHM: str = os.getenv('JWT_ALGORITHM', 'HS256')
    JWT_ISSUER: str = os.getenv('JWT_ISSUER') or ''
    JWT_AUDIENCE: str = os.getenv('JWT_AUDIENCE') or ''
    SVC_INTERNAL_KEY: str = os.getenv('SVC_INTERNAL_KEY') or ''

# Validate required security settings
if not all([os.getenv('JWT_SECRET'), os.getenv('SVC_INTERNAL_KEY')]):
    import warnings
    warnings.warn("Missing required environment variables for security")

settings = Settings()