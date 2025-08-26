
from kafka import KafkaProducer
import json
from app.core.config import settings

_producer = None

def get_producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: (v.encode("utf-8") if isinstance(v, str) else v),
            linger_ms=5,
            retries=3,
        )
    return _producer

def send(topic: str, key: str, value: dict):
    p = get_producer()
    p.send(topic, key=key, value=value)
    p.flush(5)
