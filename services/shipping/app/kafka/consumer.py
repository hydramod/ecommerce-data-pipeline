import json, threading, logging
from kafka import KafkaConsumer
from tenacity import retry, stop_after_attempt, wait_exponential
from app.core.config import settings
from app.db.session import SessionLocal  # Import SessionLocal directly
from app.db.models import Shipment, ShipmentStatus
from app.kafka.producer import emit as emit_shipping_event

logger = logging.getLogger(__name__)
_stop = threading.Event()
_thread = None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _handle_payment_event(ev: dict):
    # Create a new database session for this event
    db = SessionLocal()
    try:
        if ev.get("type") != "payment.succeeded":
            return
            
        order_id = ev.get("order_id")
        if not order_id:
            logger.warning("Payment event missing order_id")
            return
            
        shp = db.query(Shipment).filter(Shipment.order_id == int(order_id)).one_or_none()
        if not shp:
            logger.warning(f"No shipment found for order {order_id}")
            return
            
        if shp.status == ShipmentStatus.PENDING_PAYMENT:
            shp.status = ShipmentStatus.READY_TO_SHIP
            db.add(shp)
            db.commit()
            
            emit_shipping_event({
                "type": "shipping.ready",
                "order_id": shp.order_id,
                "user_email": shp.user_email,
                "shipment_id": shp.id
            })
            logger.info(f"Shipment {shp.id} marked as READY_TO_SHIP")
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to process payment event: {e}")
        raise
    finally:
        db.close()

def _run():
    consumer = None
    try:
        consumer = KafkaConsumer(
            settings.TOPIC_PAYMENT_EVENTS,
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP],
            group_id="shipping-service",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )
        
        logger.info("Payment events consumer started")
        
        for msg in consumer:
            if _stop.is_set():
                break
                
            try:
                ev = msg.value
                _handle_payment_event(ev)  # Call the handler directly
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
    _stop.clear()
    _thread = threading.Thread(target=_run, daemon=True)
    _thread.start()
    logger.info("Starting payment events consumer...")

def stop():
    _stop.set()
    logger.info("Stopping payment events consumer...")