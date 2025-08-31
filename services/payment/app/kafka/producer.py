# services/payment/app/kafka/producer.py
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
PAYMENTS_TOPIC = settings.TOPIC_PAYMENT_EVENTS

_producer = None

def _event_name_from_status(status: str) -> str:
    """Map internal payment status to a canonical event type."""
    s = (status or "").strip().lower()
    if s in {"succeeded", "success", "paid", "authorized"}:
        return "payment.succeeded"
    if s in {"failed", "declined", "error"}:
        return "payment.failed"
    return "payment.pending"

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

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
        logger.info(f"[kafka] payments-producer connected to {settings.KAFKA_BOOTSTRAP}")
    return _producer

def _with_event_type_fields(payload: dict, status: str) -> dict:
    """Ensure both `type` and `event_type` are present for cross-service compatibility."""
    t = _event_name_from_status(status)
    payload.setdefault("type", t)
    payload.setdefault("event_type", t)
    return payload

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def send(topic: str, key: str, value: dict, headers: Optional[dict] = None) -> None:
    """
    Send message to Kafka with retry logic.
    """
    try:
        producer = get_producer()
        norm_headers = []
        if headers:
            for k, v in headers.items():
                norm_headers.append((str(k), (v if isinstance(v, bytes) else str(v).encode("utf-8"))))

        future = producer.send(topic, key=key, value=value, headers=norm_headers or None)
        md = future.get(timeout=10)  # RecordMetadata
        logger.info(
            "[kafka] payments sent topic=%s partition=%s offset=%s key=%s type=%s",
            md.topic, md.partition, md.offset, key, value.get("type") or value.get("event_type")
        )
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
    trace_id: Optional[str] = None,
) -> dict:
    """
    Standardizes the payload for the real-time analytics pipeline and cross-service consumers.
    """
    base = {
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
    if trace_id:
        base["trace_id"] = str(trace_id)
    return _with_event_type_fields(base, status)

def emit_payment_event(
    *,
    order_id: str,
    payment_id: str,
    amount_cents: int,
    currency: str,
    method: str,
    status: str,
    topic: Optional[str] = None,
    trace_id: Optional[str] = None,
) -> None:
    """
    Convenience wrapper to construct and send a payment event.
    """
    evt = build_payment_event(
        payment_id=payment_id,
        order_id=order_id,
        amount_cents=amount_cents,
        currency=currency,
        method=method,
        status=status,
        trace_id=trace_id,
    )
    # Add event_type header for consumers that read from headers
    hdrs = {"event_type": evt.get("event_type", ""), "trace_id": evt.get("trace_id", "")}
    send(topic or PAYMENTS_TOPIC, key=str(order_id), value=evt, headers=hdrs)
