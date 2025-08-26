from pydantic import BaseModel
import os

class Settings(BaseModel):
    POSTGRES_DSN: str = os.getenv('POSTGRES_DSN', 'postgresql+psycopg://postgres:postgres@postgres:5432/appdb')

    # S3 / MinIO
    S3_ENDPOINT: str   = os.getenv('S3_ENDPOINT', 'http://minio:9000')
    S3_ACCESS_KEY: str = os.getenv('S3_ACCESS_KEY', 'admin')
    S3_SECRET_KEY: str = os.getenv('S3_SECRET_KEY', 'adminadmin')
    S3_BUCKET: str     = os.getenv('S3_BUCKET', 'catalog-media')
    S3_SECURE: bool    = os.getenv('S3_SECURE', 'false').lower() == 'true'

    # Auth/JWT
    JWT_SECRET: str      = os.getenv('JWT_SECRET', 'devsecret')
    JWT_ALGORITHM: str   = os.getenv('JWT_ALGORITHM', 'HS256')

    # Internal calls
    SVC_INTERNAL_KEY: str = os.getenv('SVC_INTERNAL_KEY', 'devkey')

settings = Settings()
