# services/shipping/app/kafka/producer.py
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from tenacity import retry, stop_after_attempt, wait_exponential
from app.core.config import settings
import logging
from typing import Optional

logger = logging.getLogger(__name__)

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
            acks="all",
            max_in_flight_requests_per_connection=1,
            request_timeout_ms=15000,
            security_protocol="PLAINTEXT",
        )
        logger.info(f"[kafka] shipping-producer connected to {settings.KAFKA_BOOTSTRAP}")
    return _producer

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def emit(event: dict):
    """Emit to shipping.events with retry logic."""
    try:
        p = _get_producer()
        timestamp_ms = int(time.time() * 1000)
        future = p.send(
            settings.TOPIC_SHIPPING_EVENTS,
            key=str(event.get("order_id", "")),
            value=event,
            timestamp_ms=timestamp_ms,
        )
        md = future.get(timeout=10)
        logger.info(
            "[kafka] shipping sent topic=%s partition=%s offset=%s key=%s type=%s",
            md.topic, md.partition, md.offset, event.get("order_id"), event.get("type") or event.get("event_type")
        )
    except Exception as e:
        logger.error(f"Failed to emit shipping event {event.get('type')}: {e}")
        raise

def close_producer():
    """Close Kafka producer (call during shutdown)."""
    global _producer
    if _producer is not None:
        try:
            _producer.flush(timeout=5)
            _producer.close()
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
        finally:
            _producer = None
