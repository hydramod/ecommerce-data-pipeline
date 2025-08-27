# services/payment/app/kafka/producer.py

from kafka import KafkaProducer
import json
import uuid
from datetime import datetime, timezone
from typing import Optional

from app.core.config import settings

# --- Topic defaults (override via env / Settings if you expose them there) ---
PAYMENTS_TOPIC = getattr(settings, "KAFKA_ANALYTICS_TOPIC_PAYMENTS", "payments.v1")

_producer = None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def get_producer() -> KafkaProducer:
    """
    Lazy, module-level singleton Kafka producer.
    """
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=[getattr(settings, "KAFKA_BOOTSTRAP", "kafka:9092")],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: (v.encode("utf-8") if isinstance(v, str) else v),
            linger_ms=5,
            retries=3,
            acks="all",
        )
    return _producer


def send(topic: str, key: str, value: dict) -> None:
    """
    Generic send (kept for backward compatibility with your current calls).
    """
    p = get_producer()
    p.send(topic, key=key, value=value)
    # small flush window to reduce per-event latency while keeping batching
    p.flush(timeout=5)


# -------------------------
# Analytics-friendly helpers
# -------------------------

def build_payment_event(
    *,
    payment_id: str,
    order_id: str,
    amount_cents: int,
    currency: str,
    method: str,
    status: str,  # "authorized" | "captured" | "failed" | "refunded" ...
    event_time: Optional[str] = None,  # ISO8601; default = now
) -> dict:
    """
    Standardizes the payload for the real-time analytics pipeline.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "payment_id": str(payment_id),
        "order_id": str(order_id),
        "amount": float(amount_cents) / 100.0,  # analytics uses major currency units
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
    Convenience wrapper to construct and send a payment event
    keyed by order_id (so orders & payments co-locate by key).
    """
    evt = build_payment_event(
        payment_id=payment_id,
        order_id=order_id,
        amount_cents=amount_cents,
        currency=currency,
        method=method,
        status=status,
    )
    send(topic or PAYMENTS_TOPIC, key=str(order_id), value=evt)
