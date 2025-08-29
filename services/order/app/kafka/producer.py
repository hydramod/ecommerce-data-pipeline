import atexit
import json
import os
import time
import uuid
from typing import Dict, Optional, Iterable, Tuple
from kafka import KafkaProducer
from kafka.errors import KafkaError
from tenacity import retry, stop_after_attempt, wait_exponential
from app.core.config import settings
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)

# Define message schema
class OrderEvent(BaseModel):
    event_type: str
    order_id: str
    user_id: str
    items: list
    currency: str
    total_amount: float
    status: str
    event_time: str
    event_id: str = None
    ingest_ts: str = None

class KafkaProducerWrapper:
    def __init__(self):
        self._producer = None
    
    def get_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=[settings.KAFKA_BOOTSTRAP],
                value_serializer=self._json_serializer,
                key_serializer=self._key_serializer,
                acks="all",
                linger_ms=10,
                retries=5,
                max_in_flight_requests_per_connection=1,
                compression_type="lz4",
                request_timeout_ms=15000,
                metadata_max_age_ms=30000,
            )
            atexit.register(self._close_producer)
        return self._producer
    
    def _json_serializer(self, v: Dict) -> bytes:
        return json.dumps(v, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    
    def _key_serializer(self, v) -> Optional[bytes]:
        if v is None:
            return None
        if isinstance(v, bytes):
            return v
        return str(v).encode("utf-8")
    
    def _close_producer(self):
        if self._producer is not None:
            try:
                self._producer.flush(5)
                self._producer.close(5)
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
            finally:
                self._producer = None
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def send(self, topic: str, key: Optional[str], value: Dict, headers: Iterable[Tuple[str, bytes]] = None) -> None:
        producer = self.get_producer()
        try:
            # Validate message schema
            if topic == "order.events":
                OrderEvent(**value)
            
            fut = producer.send(topic, key=key, value=value, headers=list(headers) if headers else None)
            fut.get(timeout=10)
        except KafkaError as e:
            logger.error(f"[kafka] send failed: topic={topic} key={key} err={e}")
            raise
        except Exception as e:
            logger.error(f"[kafka] unexpected error: {e}")
            raise

# Global instance
_producer_wrapper = KafkaProducerWrapper()

def send(topic: str, key: Optional[str], value: Dict, headers: Iterable[Tuple[str, bytes]] = None) -> None:
    _producer_wrapper.send(topic, key, value, headers)

def publish_order_event(event: Dict, topic: str = "order.events") -> None:
    """
    Convenience wrapper for order events.
    Ensures required envelope fields & consistent keying by order_id.
    """
    # do not mutate caller's dict
    payload = dict(event)

    # Ensure minimal envelope for analytics
    payload.setdefault("event_id", str(uuid.uuid4()))
    payload.setdefault("ingest_ts", _utc_iso())
    
    order_id = payload.get("order_id")
    if not order_id:
        # Fall back to keying by event_id to avoid None keys
        order_id = payload["event_id"]

    send(topic=topic, key=str(order_id), value=payload)

def _utc_iso() -> str:
    # RFC3339-ish UTC timestamp
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def check_kafka_health():
    """Check Kafka connection health"""
    try:
        producer = _producer_wrapper.get_producer()
        # Try to get metadata to verify connection
        producer.list_topics(timeout=5)
        return True
    except Exception as e:
        logger.error(f"Kafka health check failed: {e}")
        return False