from fastapi import APIRouter
from pydantic import BaseModel, EmailStr
from app.kafka.consumer import send_email

router = APIRouter()

class TestEmail(BaseModel):
    to: EmailStr
    subject: str
    body: str

@router.post("/notifications/v1/test-email")
def test_email(payload: TestEmail):
    send_email(payload.to, payload.subject, payload.body)
    return {"sent": True}
