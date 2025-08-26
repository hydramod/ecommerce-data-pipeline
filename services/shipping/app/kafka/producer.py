import json
from kafka import KafkaProducer
from app.core.config import settings

_producer = None

def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: (v.encode("utf-8") if isinstance(v, str) else v),
            linger_ms=10,
            retries=5,
        )
    return _producer

def emit(event: dict):
    """Emit to shipping.events (configurable)."""
    p = _get_producer()
    p.send(settings.TOPIC_SHIPPING_EVENTS, key=str(event.get("order_id", "")), value=event)
    p.flush(5)
