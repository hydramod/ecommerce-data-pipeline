from fastapi import APIRouter, Depends, HTTPException, Header, status
from pydantic import BaseModel, Field, validator
from app.kafka.producer import send, emit_payment_event
from app.core.config import settings
import jwt
import logging
from typing import Optional

logger = logging.getLogger(__name__)
router = APIRouter()

def get_identity_dep(authorization: str | None = Header(default=None, alias="Authorization")) -> dict:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    if not settings.JWT_SECRET:
        raise HTTPException(status_code=500, detail="JWT configuration missing")
    
    token = authorization.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, settings.JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {e}")
        raise HTTPException(status_code=401, detail="Invalid token")
    except Exception as e:
        logger.error(f"Token validation error: {e}")
        raise HTTPException(status_code=401, detail="Invalid token")
    
    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid access token")
    return payload

class CreateIntent(BaseModel):
    order_id: int = Field(..., gt=0)
    amount_cents: Optional[int] = Field(None, gt=0)
    currency: str = Field("USD", min_length=3, max_length=3)
    
    @validator('currency')
    def currency_uppercase(cls, v):
        return v.upper()

class IntentResponse(BaseModel):
    payment_id: str
    client_secret: str

class MockSucceed(BaseModel):
    order_id: int = Field(..., gt=0)
    amount_cents: int = Field(..., gt=0)
    currency: str = Field("USD", min_length=3, max_length=3)
    
    @validator('currency')
    def currency_uppercase(cls, v):
        return v.upper()

@router.post("/v1/payments/create-intent", response_model=IntentResponse)
def create_intent(payload: CreateIntent, identity: dict = Depends(get_identity_dep)):
    # In a real implementation, this would call Stripe/PayPal/etc.
    try:
        # Mock: return a fake payment id & secret
        pid = f"pay_{payload.order_id}"
        return IntentResponse(payment_id=pid, client_secret=f"secret_{payload.order_id}")
    except Exception as e:
        logger.error(f"Failed to create payment intent: {e}")
        raise HTTPException(status_code=500, detail="Failed to create payment intent")

@router.post("/v1/payments/mock-succeed")
def mock_succeed(
    payload: MockSucceed, 
    x_internal_key: Optional[str] = Header(None, alias="X-Internal-Key"),
    identity: Optional[dict] = Depends(get_identity_dep, use_cache=False)
):
    # Allow either authenticated user OR internal key
    is_authenticated = (
        identity is not None or 
        (x_internal_key and x_internal_key == settings.SVC_INTERNAL_KEY)
    )
    
    if not is_authenticated:
        raise HTTPException(status_code=403, detail="Forbidden")
    
    try:
        # Get user email from identity if available
        user_email = identity.get("sub") if identity else ""
        
        send("payments.events", key=str(payload.order_id), value={
            "type": "payment.succeeded",
            "order_id": payload.order_id,
            "amount_cents": payload.amount_cents,
            "currency": payload.currency,
            "user_email": user_email,  # Add this field
        })
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Failed to send payment success event: {e}")
        raise HTTPException(status_code=500, detail="Failed to process payment success")