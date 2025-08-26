import io, uuid
from minio import Minio
from app.core.config import settings

def _client():
    return Minio(settings.S3_ENDPOINT.replace('http://','').replace('https://',''), access_key=settings.S3_ACCESS_KEY, secret_key=settings.S3_SECRET_KEY, secure=settings.S3_SECURE)

def ensure_bucket():
    c = _client()
    if not c.bucket_exists(settings.S3_BUCKET):
        c.make_bucket(settings.S3_BUCKET)

def upload_bytes(data: bytes, content_type: str, ext: str = ''):
    ensure_bucket()
    key = f"products/{uuid.uuid4().hex}{ext}"
    c = _client()
    c.put_object(settings.S3_BUCKET, key, io.BytesIO(data), length=len(data), content_type=content_type)
    scheme = 'https' if settings.S3_SECURE else 'http'
    url = f"{scheme}://{settings.S3_ENDPOINT.replace('http://','').replace('https://','')}/{settings.S3_BUCKET}/{key}"
    return key, url
