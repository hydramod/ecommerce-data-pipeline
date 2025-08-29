from pydantic import BaseModel
import os

class Settings(BaseModel):
    POSTGRES_DSN: str = os.getenv('POSTGRES_DSN', 'postgresql+psycopg://postgres:postgres@postgres:5432/appdb')
    S3_ENDPOINT: str = os.getenv('S3_ENDPOINT', 'http://minio:9000')
    S3_ACCESS_KEY: str = os.getenv('S3_ACCESS_KEY') or ''
    S3_SECRET_KEY: str = os.getenv('S3_SECRET_KEY') or ''
    S3_BUCKET: str = os.getenv('S3_BUCKET', 'catalog-media')
    S3_SECURE: bool = os.getenv('S3_SECURE', 'false').lower() == 'true'
    JWT_SECRET: str = os.getenv('JWT_SECRET') or ''
    JWT_ALGORITHM: str = os.getenv('JWT_ALGORITHM', 'HS256')
    SVC_INTERNAL_KEY: str = os.getenv('SVC_INTERNAL_KEY') or ''

# Validate required security settings
if not all([os.getenv('JWT_SECRET'), os.getenv('SVC_INTERNAL_KEY'), os.getenv('S3_ACCESS_KEY'), os.getenv('S3_SECRET_KEY')]):
    import warnings
    warnings.warn("Missing required environment variables for security")

settings = Settings()
