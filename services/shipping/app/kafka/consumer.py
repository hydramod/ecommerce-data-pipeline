# services/shipping/app/kafka/consumer.py
import json
import threading
import logging
from typing import Optional, Iterable, Tuple

from kafka import KafkaConsumer
from tenacity import retry, stop_after_attempt, wait_exponential

from app.core.config import settings
from app.db.session import SessionLocal
from app.db.models import Shipment, ShipmentStatus
from app.kafka.producer import emit as emit_shipping_event

logger = logging.getLogger(__name__)

_stop = threading.Event()
_thread = None

def _decode_header(headers: Optional[Iterable[Tuple[str, Optional[bytes]]]], key: str) -> Optional[str]:
    if not headers:
        return None
    for k, v in headers:
        if k == key:
            try:
                return None if v is None else v.decode("utf-8")
            except Exception:
                return str(v)
    return None

def _event_type(ev: dict, headers: Optional[Iterable[Tuple[str, Optional[bytes]]]]) -> Optional[str]:
    """
    Unified way to read event type:
    - prefer body 'event_type'
    - else body 'type'
    - else Kafka header 'event_type'
    """
    val = ev.get("event_type") or ev.get("type") or _decode_header(headers, "event_type")
    return (val or "").strip()

def _parse_order_id(raw) -> Optional[int]:
    if raw is None:
        return None
    try:
        return int(raw)
    except Exception:
        return None

def _is_email(s: Optional[str]) -> bool:
    return isinstance(s, str) and "@" in s and "." in s

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _handle_order_created(ev: dict, headers=None):
    """Handle order.created events with proper field mapping."""
    et = _event_type(ev, headers).lower()
    if et not in ("order.created", "order_created"):
        return

    db = SessionLocal()
    try:
        oid = _parse_order_id(ev.get("order_id"))
        if not oid:
            logger.warning("order.created missing/invalid order_id; skipping")
            return

        existing = db.query(Shipment).filter(Shipment.order_id == oid).one_or_none()
        if existing:
            logger.info(f"[order.created] Shipment already exists for order_id={oid}; skipping")
            return

        ship = ev.get("shipping") or {}
        # Prefer user_email; only fallback to user_id if it looks like an email
        user_email = ev.get("user_email") or (ev.get("user_id") if _is_email(ev.get("user_id")) else "")

        if not user_email:
            logger.warning(f"[order.created] No user email found for order_id={oid}")
            return

        shp = Shipment(
            order_id=oid,
            user_email=user_email,
            address_line1=ship.get("address_line1", ""),
            address_line2=ship.get("address_line2", ""),
            city=ship.get("city", ""),
            country=(ship.get("country") or "").upper(),
            postcode=ship.get("postcode", ""),
            status=ShipmentStatus.PENDING_PAYMENT,
        )
        db.add(shp)
        db.commit()
        logger.info(f"[order.created] Shipment id={shp.id} created for order_id={oid} → PENDING_PAYMENT")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed processing order.created: {e}")
        raise
    finally:
        db.close()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _handle_payment_succeeded(ev: dict, headers=None):
    """
    On payment.succeeded, mark Shipment READY_TO_SHIP and emit shipping.ready.
    Accepts either body 'type' or header 'event_type'.
    """
    et = _event_type(ev, headers)
    if et != "payment.succeeded":
        return

    db = SessionLocal()
    try:
        oid = _parse_order_id(ev.get("order_id"))
        if not oid:
            logger.warning("payment.succeeded missing/invalid order_id; skipping")
            return

        shp = db.query(Shipment).filter(Shipment.order_id == oid).one_or_none()
        if not shp:
            logger.warning(f"[payment.succeeded] No shipment found for order_id={oid}")
            return

        if shp.status == ShipmentStatus.PENDING_PAYMENT:
            shp.status = ShipmentStatus.READY_TO_SHIP
            db.add(shp)
            db.commit()

            emit_shipping_event({
                "type": "shipping.ready",
                "event_type": "shipping.ready",
                "order_id": shp.order_id,
                "user_email": shp.user_email,
                "shipment_id": shp.id,
            })
            logger.info(f"[payment.succeeded] Shipment id={shp.id} → READY_TO_SHIP")
        else:
            logger.info(f"[payment.succeeded] Shipment id={shp.id} already {shp.status}; noop")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed processing payment.succeeded: {e}")
        raise
    finally:
        db.close()

def _run():
    consumer = None
    try:
        consumer = KafkaConsumer(
            settings.TOPIC_PAYMENT_EVENTS,      # e.g. "payments.events"
            settings.TOPIC_ORDER_EVENTS,        # e.g. "order.events"
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP],
            group_id="shipping-service",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            enable_auto_commit=True,
            auto_offset_reset="earliest",  # dev-friendly; switch to "latest" in prod
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )

        logger.info(
            f"Shipping consumer started; topics=[{settings.TOPIC_PAYMENT_EVENTS}, {settings.TOPIC_ORDER_EVENTS}]"
        )

        for msg in consumer:
            if _stop.is_set():
                break
            try:
                ev = msg.value or {}
                headers = getattr(msg, "headers", None)
                _handle_order_created(ev, headers)
                _handle_payment_succeeded(ev, headers)
            except Exception as e:
                logger.error(f"Error processing message on topic={msg.topic}: {e}")

    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")
    finally:
        if consumer:
            consumer.close()
        logger.info("Shipping consumer stopped")

def start():
    global _thread
    if _thread and _thread.is_alive():
        return
    _stop.clear()
    _thread = threading.Thread(target=_run, daemon=True)
    _thread.start()
    logger.info("Starting shipping consumer thread...")

def stop():
    _stop.set()
    logger.info("Stopping shipping consumer thread...")
