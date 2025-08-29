# File: notifications/app/api/routes.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
from app.kafka.consumer import send_email
import time
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

# Simple rate limiting
_email_attempts = defaultdict(list)
_RATE_LIMIT = 5  # emails per minute
_RATE_WINDOW = 60  # seconds

class TestEmail(BaseModel):
    to: EmailStr
    subject: str
    body: str

@router.post("/notifications/v1/test-email")
def test_email(payload: TestEmail):
    # Rate limiting
    current_time = time.time()
    attempts = _email_attempts[payload.to]
    
    # Remove old attempts
    attempts = [t for t in attempts if current_time - t < _RATE_WINDOW]
    _email_attempts[payload.to] = attempts
    
    if len(attempts) >= _RATE_LIMIT:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    
    attempts.append(current_time)
    
    # Send email
    success = send_email(payload.to, payload.subject, payload.body)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to send email")
    
    logger.info(f"Test email sent to {payload.to}")
    return {"sent": True}
