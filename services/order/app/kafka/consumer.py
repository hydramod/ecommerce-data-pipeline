import threading
import time
import json
import logging
from kafka import KafkaConsumer
from sqlalchemy.orm import Session
from app.core.config import settings
from app.db.session import SessionLocal  # Import SessionLocal directly
from app.db.models import Order
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)
_stop_event = threading.Event()
_thread = None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def call_catalog_commit(items, db):
    """Call catalog service to commit inventory with retry logic"""
    if not settings.SVC_INTERNAL_KEY:
        logger.error("SVC_INTERNAL_KEY not configured, cannot commit inventory")
        raise Exception("Service configuration incomplete")
        
    try:
        with httpx.Client(timeout=5.0) as client:
            resp = client.post(
                f"{settings.CATALOG_BASE}/catalog/v1/inventory/commit", 
                json={"items": items}, 
                headers={"X-Internal-Key": settings.SVC_INTERNAL_KEY}
            )
            if resp.status_code != 200:
                logger.error(f"Failed to commit inventory: {resp.text}")
                raise Exception(f"Catalog commit failed: {resp.text}")
            return True
    except httpx.RequestError as e:
        logger.error(f"Catalog service unavailable: {e}")
        raise

def process_event(ev: dict):
    # Create a new database session for this event
    db = SessionLocal()
    try:
        if ev.get("type") == "payment.succeeded":
            order_id = ev.get("order_id")
            if not order_id:
                logger.warning("Received payment.succeeded event without order_id")
                return
                
            order = db.get(Order, order_id)
            if not order:
                logger.warning(f"Order {order_id} not found for payment.succeeded event")
                return
            
            if order.status == "PAID":
                logger.info(f"Order {order_id} is already PAID, skipping")
                return
                
            # Commit inventory in catalog
            items = [{"product_id": it.product_id, "qty": it.qty} for it in order.items]
            try:
                call_catalog_commit(items, db)
                order.status = "PAID"
                db.add(order)
                db.commit()
                logger.info(f"Order {order_id} marked as PAID")
            except Exception as e:
                logger.error(f"Failed to process payment for order {order_id}: {e}")
                db.rollback()
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        db.rollback()
    finally:
        db.close()

def run_loop():
    consumer = None
    try:
        consumer = KafkaConsumer(
            "payment.events",
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP],
            group_id="order-service",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )
        
        logger.info("Payment events consumer started successfully")
        
        while not _stop_event.is_set():
            for msg in consumer:
                if _stop_event.is_set():
                    break
                    
                try:
                    ev = msg.value
                    process_event(ev)  # Pass the session to process_event
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")
    finally:
        if consumer:
            consumer.close()
        logger.info("Payment events consumer stopped")

def start():
    global _thread
    if _thread and _thread.is_alive():
        return
    _stop_event.clear()
    _thread = threading.Thread(target=run_loop, daemon=True)
    _thread.start()
    logger.info("Payment events consumer starting...")

def stop():
    _stop_event.set()
    logger.info("Stopping payment events consumer...")