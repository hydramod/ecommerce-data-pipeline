import json, threading
from kafka import KafkaConsumer
from app.core.config import settings
from app.db.session import SessionLocal
from app.db.models import Shipment, ShipmentStatus
from app.kafka.producer import emit as emit_shipping_event

_stop = threading.Event()
_thread = None

def _handle_payment_event(ev: dict, db):
    if ev.get("type") != "payment.succeeded":
        return
    order_id = ev.get("order_id")
    if not order_id:
        return
    shp = db.query(Shipment).filter(Shipment.order_id == int(order_id)).one_or_none()
    if not shp:
        return
    if shp.status == ShipmentStatus.PENDING_PAYMENT:
        shp.status = ShipmentStatus.READY_TO_SHIP
        db.add(shp); db.commit()
        emit_shipping_event({
            "type": "shipping.ready",
            "order_id": shp.order_id,
            "user_email": shp.user_email,
            "shipment_id": shp.id
        })

def _run():
    consumer = KafkaConsumer(
        settings.TOPIC_PAYMENT_EVENTS,
        bootstrap_servers=[settings.KAFKA_BOOTSTRAP],
        group_id="shipping-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    db = SessionLocal()
    try:
        for msg in consumer:
            if _stop.is_set():
                break
            ev = msg.value
            _handle_payment_event(ev, db)
    finally:
        db.close()
        consumer.close()

def start():
    global _thread
    if _thread and _thread.is_alive():
        return
    _stop.clear()
    _thread = threading.Thread(target=_run, daemon=True)
    _thread.start()

def stop():
    _stop.set()
