import json
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
        )
    return _producer

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def emit(event: dict):
    """Emit to shipping.events with retry logic."""
    try:
        p = _get_producer()
        future = p.send(settings.TOPIC_SHIPPING_EVENTS, key=str(event.get("order_id", "")), value=event)
        future.get(timeout=10)  # Wait for confirmation
        logger.debug(f"Successfully emitted shipping event: {event.get('type')}")
    except KafkaError as e:
        logger.error(f"Failed to emit shipping event: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error emitting shipping event: {e}")
        raise

def close_producer():
    """Close Kafka producer (call during shutdown)"""
    global _producer
    if _producer is not None:
        try:
            _producer.flush(timeout=5)
            _producer.close()
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
        finally:
            _producer = None