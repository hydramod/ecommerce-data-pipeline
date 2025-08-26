
import threading, time, json
from kafka import KafkaConsumer
from sqlalchemy.orm import Session
from app.core.config import settings
from app.db.session import SessionLocal
from app.db.models import Order, OrderItem
import httpx

_stop_event = threading.Event()
_thread = None

def process_event(ev: dict, db: Session):
    if ev.get("type") == "payment.succeeded":
        order_id = ev.get("order_id")
        order = db.get(Order, order_id)
        if not order: return
        # Commit inventory in catalog
        items = [{"product_id": it.product_id, "qty": it.qty} for it in order.items]
        with httpx.Client(timeout=5.0) as client:
            client.post(f"{settings.CATALOG_BASE}/catalog/v1/inventory/commit", json={"items": items}, headers={"X-Internal-Key": settings.SVC_INTERNAL_KEY})
        order.status = "PAID"
        db.add(order); db.commit()

def run_loop():
    consumer = KafkaConsumer(
        "payment.events",
        bootstrap_servers=[settings.KAFKA_BOOTSTRAP],
        group_id="order-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    db = SessionLocal()
    try:
        for msg in consumer:
            if _stop_event.is_set(): break
            ev = msg.value
            process_event(ev, db)
    finally:
        db.close()
        consumer.close()

def start():
    global _thread
    if _thread and _thread.is_alive(): return
    _stop_event.clear()
    _thread = threading.Thread(target=run_loop, daemon=True)
    _thread.start()

def stop():
    _stop_event.set()
