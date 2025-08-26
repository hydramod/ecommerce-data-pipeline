
from fastapi import APIRouter
from pydantic import BaseModel
from app.kafka.producer import send

router = APIRouter()

class CreateIntent(BaseModel):
    order_id: int
    amount_cents: int | None = None
    currency: str = "USD"

class IntentResponse(BaseModel):
    payment_id: str
    client_secret: str

@router.post("/v1/payments/create-intent", response_model=IntentResponse)
def create_intent(payload: CreateIntent):
    # Mock: return a fake payment id & secret; real flow would call Stripe/etc.
    pid = f"pay_{payload.order_id}"
    return IntentResponse(payment_id=pid, client_secret=f"secret_{payload.order_id}")

class MockSucceed(BaseModel):
    order_id: int
    amount_cents: int
    currency: str = "USD"

@router.post("/v1/payments/mock-succeed")
def mock_succeed(payload: MockSucceed):
    # Emit Kafka event that order service will consume
    send("payment.events", key=str(payload.order_id), value={
        "type": "payment.succeeded",
        "order_id": payload.order_id,
        "amount_cents": payload.amount_cents,
        "currency": payload.currency,
    })
    return {"status": "ok"}
