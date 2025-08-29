# File: catalog/app/services/storage.py
import io, uuid, logging
from minio import Minio
from minio.error import S3Error
from fastapi import HTTPException
from app.core.config import settings

logger = logging.getLogger(__name__)

def _client():
    return Minio(
        settings.S3_ENDPOINT.replace('http://','').replace('https://',''), 
        access_key=settings.S3_ACCESS_KEY, 
        secret_key=settings.S3_SECRET_KEY, 
        secure=settings.S3_SECURE
    )

def ensure_bucket():
    try:
        c = _client()
        if not c.bucket_exists(settings.S3_BUCKET):
            c.make_bucket(settings.S3_BUCKET)
    except S3Error as e:
        logger.error(f"S3 bucket operation failed: {e}")
        raise HTTPException(status_code=500, detail="Storage service unavailable")
    except Exception as e:
        logger.error(f"Unexpected error ensuring bucket: {e}")
        raise HTTPException(status_code=500, detail="Storage service error")

def upload_bytes(data: bytes, content_type: str, ext: str = ''):
    try:
        ensure_bucket()
        key = f"products/{uuid.uuid4().hex}{ext}"
        c = _client()
        c.put_object(settings.S3_BUCKET, key, io.BytesIO(data), length=len(data), content_type=content_type)
        scheme = 'https' if settings.S3_SECURE else 'http'
        url = f"{scheme}://{settings.S3_ENDPOINT.replace('http://','').replace('https://','')}/{settings.S3_BUCKET}/{key}"
        return key, url
    except S3Error as e:
        logger.error(f"S3 upload failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to upload image")
    except Exception as e:
        logger.error(f"Unexpected upload error: {e}")
        raise HTTPException(status_code=500, detail="Upload failed")
