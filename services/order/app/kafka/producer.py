# services/order/app/kafka/producer.py
import atexit
import json
import os
import time
import uuid
from typing import Dict, Optional, Iterable, Tuple

from kafka import KafkaProducer
from kafka.errors import KafkaError

from app.core.config import settings

# ---- Config ----
KAFKA_BOOTSTRAP = getattr(settings, "KAFKA_BOOTSTRAP", os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
TOPIC_ORDERS    = os.getenv("KAFKA_TOPIC_ORDERS", "orders.v1")

# Optional: stamp a schema version into headers so downstream consumers can branch if needed
DEFAULT_HEADERS: Iterable[Tuple[str, bytes]] = (
    ("schema", b"orders.v1.json"),
    ("version", b"1"),
)

_producer: Optional[KafkaProducer] = None


def _json_serializer(v: Dict) -> bytes:
    # Compact JSON for smaller payloads; ensure ASCII-safe
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _key_serializer(v) -> Optional[bytes]:
    if v is None:
        return None
    if isinstance(v, bytes):
        return v
    return str(v).encode("utf-8")


def get_producer() -> KafkaProducer:
    """Singleton Kafka producer with sensible defaults for low latency + resilience."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=_json_serializer,
            key_serializer=_key_serializer,
            acks="all",                # strongest durability
            linger_ms=10,              # small batching without big latency
            retries=5,                 # retry transient errors
            max_in_flight_requests_per_connection=1,  # preserve order with retries
            compression_type="lz4",    # smaller payloads, lower broker IO
            request_timeout_ms=15000,
            metadata_max_age_ms=30000,
        )
        atexit.register(_close_producer)
    return _producer


def _close_producer():
    global _producer
    if _producer is not None:
        try:
            _producer.flush(5)
            _producer.close(5)
        except Exception:
            pass
        finally:
            _producer = None


def send(topic: str, key: Optional[str], value: Dict, headers: Iterable[Tuple[str, bytes]] = DEFAULT_HEADERS) -> None:
    """
    Backward-compatible send used by existing code.
    Adds optional Kafka headers and basic error logging.
    """
    prod = get_producer()
    try:
        fut = prod.send(topic, key=key, value=value, headers=list(headers) if headers else None)
        # Make this effectively synchronous for demo determinism; switch to async if you prefer
        fut.get(timeout=10)
    except KafkaError as e:
        # In real services, push this to your logger
        # (avoid raising to keep the order flow responsive even if analytics path is flaky)
        print(f"[kafka] send failed: topic={topic} key={key} err={e}")
    except Exception as e:
        print(f"[kafka] unexpected error: {e}")


# ---------- Convenience helpers (non-breaking) ----------

def publish_order_event(event: Dict, topic: str = TOPIC_ORDERS) -> None:
    """
    Convenience wrapper for order events.
    Ensures required envelope fields & consistent keying by order_id.
    """
    # do not mutate caller's dict
    payload = dict(event)

    # Ensure minimal envelope for analytics
    payload.setdefault("event_id", str(uuid.uuid4()))
    payload.setdefault("ingest_ts", _utc_iso())
    # If your order model already sets these, this will be a no-op:
    #   - event_type: 'order_created' | 'order_updated' | 'order_cancelled' | 'order_paid'
    #   - order_id: string/uuid
    #   - user_id, items, totals, currency, status, event_time

    order_id = payload.get("order_id")
    if not order_id:
        # Fall back to keying by event_id to avoid None keys
        order_id = payload["event_id"]

    send(topic=topic, key=str(order_id), value=payload)


def _utc_iso() -> str:
    # RFC3339-ish UTC timestamp
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
