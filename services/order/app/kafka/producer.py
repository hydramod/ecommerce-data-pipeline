# services/order/app/kafka/producer.py
import atexit
import json
import time
import uuid
import logging
from typing import Dict, Optional, Iterable, Tuple

from kafka import KafkaProducer
from kafka.errors import KafkaError
from tenacity import retry, stop_after_attempt, wait_exponential
from pydantic import BaseModel

from app.core.config import settings

logger = logging.getLogger(__name__)

def _normalize_headers(headers):
    """Accept dict or iterable of pairs; return List[Tuple[str, bytes]] or None."""
    if headers is None:
        return None
    items = headers.items() if isinstance(headers, dict) else headers
    out = []
    for k, v in items:
        if v is None:
            b = b""
        elif isinstance(v, bytes):
            b = v
        else:
            b = str(v).encode("utf-8")
        out.append((str(k), b))
    return out

# ---- Schema ----
class OrderEvent(BaseModel):
    event_type: str
    order_id: str
    user_id: str
    items: list
    currency: str
    total_amount: float
    status: str
    event_time: str
    event_id: Optional[str] = None
    ingest_ts: Optional[str] = None

ORDER_TOPIC = settings.TOPIC_ORDER_EVENTS

def _compression_with_fallback() -> str:
    try:
        import lz4  # noqa: F401
        return "lz4"
    except Exception:
        return "gzip"

class KafkaProducerWrapper:
    def __init__(self):
        self._producer: Optional[KafkaProducer] = None

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
                compression_type=_compression_with_fallback(),
                request_timeout_ms=15000,
                metadata_max_age_ms=30000,
            )
            atexit.register(self._close_producer)
            logger.info(f"[kafka] orders-producer connected to {settings.KAFKA_BOOTSTRAP}")
        return self._producer

    @staticmethod
    def _json_serializer(v: Dict) -> bytes:
        return json.dumps(v, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    @staticmethod
    def _key_serializer(v) -> Optional[bytes]:
        if v is None:
            return None
        return v if isinstance(v, bytes) else str(v).encode("utf-8")

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
    def send(self, topic, key, value, headers=None) -> None:
        producer = self.get_producer()
        try:
            if topic == ORDER_TOPIC:
                OrderEvent(**value)  # schema check

            norm_headers = _normalize_headers(headers)
            logger.info(f"[kafka] sending to topic={topic} key={key}")

            fut = producer.send(topic, key=key, value=value, headers=norm_headers)
            md = fut.get(timeout=10)
            logger.info(
                "[kafka] orders sent topic=%s partition=%s offset=%s key=%s type=%s",
                md.topic, md.partition, md.offset, key, value.get("event_type")
            )
        except AssertionError as e:
            logger.error(
                f"[kafka] AssertionError (likely bad headers or types). "
                f"topic={topic} key_type={type(key)} headers_type={type(headers)} headers={headers} err={e}"
            )
            raise
        except KafkaError as e:
            logger.error(f"[kafka] send failed: topic={topic} key={key} err={e}")
            raise
        except Exception as e:
            logger.error(f"[kafka] unexpected error: {e}")
            raise


# Global instance
_producer_wrapper = KafkaProducerWrapper()

def send(topic: str, key: Optional[str], value: Dict, headers: Optional[Iterable[Tuple[str, bytes]]] = None) -> None:
    _producer_wrapper.send(topic, key, value, headers)

def publish_order_event(event: Dict, topic: str = ORDER_TOPIC, headers: Optional[dict] = None) -> None:
    """
    Convenience wrapper for order events.
    Ensures required envelope fields & consistent keying by order_id.
    Also injects an event_type header for consumers that look at headers.
    """
    payload = dict(event)
    payload.setdefault("event_id", str(uuid.uuid4()))
    payload.setdefault("ingest_ts", _utc_iso())
    payload.setdefault("event_time", _utc_iso())

    order_id = payload.get("order_id") or payload["event_id"]

    hdrs = {"event_type": payload.get("event_type", "")}
    if headers:
        hdrs.update(headers)

    send(topic=topic, key=str(order_id), value=payload, headers=hdrs)

def _utc_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def check_kafka_health() -> bool:
    try:
        _producer_wrapper.get_producer().list_topics(timeout=5)
        return True
    except Exception as e:
        logger.error(f"Kafka health check failed: {e}")
        return False
