from kafka import KafkaProducer
from kafka.errors import KafkaError
from tenacity import retry, stop_after_attempt, wait_exponential
import json
import uuid
from datetime import datetime, timezone
from typing import Optional
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)

# --- Topic defaults ---
PAYMENTS_TOPIC = "payments.v1"

_producer = None

def get_producer() -> KafkaProducer:
    """
    Lazy, module-level singleton Kafka producer with proper configuration.
    """
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: (v.encode("utf-8") if isinstance(v, str) else v),
            linger_ms=5,
            retries=3,
            acks="all",
            max_in_flight_requests_per_connection=1,
            request_timeout_ms=15000,
        )
    return _producer

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def send(topic: str, key: str, value: dict) -> None:
    """
    Send message to Kafka with retry logic.
    """
    try:
        producer = get_producer()
        future = producer.send(topic, key=key, value=value)
        # Wait for message to be delivered (synchronous for reliability)
        future.get(timeout=10)
        logger.debug(f"Successfully sent message to {topic} with key {key}")
    except KafkaError as e:
        logger.error(f"Failed to send message to Kafka: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error sending to Kafka: {e}")
        raise

def build_payment_event(
    *,
    payment_id: str,
    order_id: str,
    amount_cents: int,
    currency: str,
    method: str,
    status: str,
    event_time: Optional[str] = None,
) -> dict:
    """
    Standardizes the payload for the real-time analytics pipeline.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "payment_id": str(payment_id),
        "order_id": str(order_id),
        "amount": float(amount_cents) / 100.0,
        "currency": currency,
        "method": method,
        "status": status,
        "event_time": event_time or _utc_now_iso(),
        "ingest_ts": _utc_now_iso(),
    }

def emit_payment_event(
    *,
    order_id: str,
    payment_id: str,
    amount_cents: int,
    currency: str,
    method: str,
    status: str,
    topic: Optional[str] = None,
) -> None:
    """
    Convenience wrapper to construct and send a payment event.
    """
    try:
        evt = build_payment_event(
            payment_id=payment_id,
            order_id=order_id,
            amount_cents=amount_cents,
            currency=currency,
            method=method,
            status=status,
        )
        send(topic or PAYMENTS_TOPIC, key=str(order_id), value=evt)
    except Exception as e:
        logger.error(f"Failed to emit payment event: {e}")
        # Don't raise exception to avoid breaking the main flow