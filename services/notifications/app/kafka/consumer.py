# File: notifications/app/kafka/consumer.py
import json, threading, smtplib, logging, time
from collections import OrderedDict
from email.mime.text import MIMEText
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from app.core.config import settings

logger = logging.getLogger(__name__)
_stop = threading.Event()
_thread = None

# LRU cache with expiration
_order_email = OrderedDict()
_MAX_CACHE_SIZE = 10000
_CACHE_TTL = 86400  # 24 hours

def send_email(to: str, subject: str, body: str):
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = settings.FROM_EMAIL
        msg["To"] = to
        
        with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=10) as s:
            s.sendmail(settings.FROM_EMAIL, [to], msg.as_string())
        
        logger.info(f"Email sent to {to}: {subject}")
        return True
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error sending to {to}: {e}")
    except Exception as e:
        logger.error(f"Failed to send email to {to}: {e}")
    return False

def cleanup_cache():
    """Remove expired cache entries"""
    current_time = time.time()
    expired_keys = [
        k for k, v in _order_email.items() 
        if current_time - v['timestamp'] > _CACHE_TTL
    ]
    for key in expired_keys:
        _order_email.pop(key, None)

def _handle(ev: dict):
    try:
        # Clean up cache if needed
        if len(_order_email) > _MAX_CACHE_SIZE:
            cleanup_cache()
            
        t = ev.get("type")
        order_id = ev.get("order_id")
        
        if t == "order.created":
            if order_id and ev.get("user_email"):
                _order_email[order_id] = {
                    'email': ev["user_email"],
                    'timestamp': time.time()
                }
            if ev.get("user_email"):
                send_email(ev["user_email"], "Order received",
                           f"We received your order {ev['order_id']} for {ev.get('amount_cents','?')} cents.")
        
        elif t == "payment.succeeded":
            email = ev.get("user_email")
            if not email and order_id:
                cache_entry = _order_email.get(order_id)
                if cache_entry:
                    email = cache_entry['email']
            if email:
                send_email(email, "Payment received",
                           f"Payment for order {order_id} succeeded.")
        
        elif t == "shipping.ready":
            email = ev.get("user_email")
            if not email and order_id:
                cache_entry = _order_email.get(order_id)
                if cache_entry:
                    email = cache_entry['email']
            if email:
                send_email(email, "Order ready to ship",
                           f"Your order {order_id} is ready to ship.")
        
        elif t == "shipping.dispatched":
            email = ev.get("user_email")
            if not email and order_id:
                cache_entry = _order_email.get(order_id)
                if cache_entry:
                    email = cache_entry['email']
            if email:
                send_email(email, "Order dispatched",
                           f"Your order {order_id} has been dispatched. "
                           f"Tracking: {ev.get('tracking_number','TBA')}")
    
    except Exception as e:
        logger.error(f"Error handling event {ev.get('type')}: {e}")

def _run():
    try:
        consumer = KafkaConsumer(
            settings.TOPIC_ORDER_EVENTS,
            settings.TOPIC_PAYMENT_EVENTS,
            settings.TOPIC_SHIPPING_EVENTS,
            bootstrap_servers=[settings.KAFKA_BOOTSTRAP],
            group_id="notifications-service",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_interval_ms=300000,
            retry_backoff_ms=1000,
            max_poll_records=100,
        )
        logger.info("Kafka consumer started")
        
        while not _stop.is_set():
            try:
                # Poll with timeout to allow graceful shutdown
                messages = consumer.poll(timeout_ms=1000)
                for tp, msgs in messages.items():
                    for msg in msgs:
                        _handle(msg.value)
                consumer.commit_async()
            except KafkaError as e:
                logger.error(f"Kafka poll error: {e}")
            except Exception as e:
                logger.error(f"Unexpected error in consumer loop: {e}")
                
    except Exception as e:
        logger.error(f"Kafka consumer failed to start: {e}")
    finally:
        try:
            consumer.close()
        except:
            pass
        logger.info("Kafka consumer stopped")

def start():
    global _thread
    if _thread and _thread.is_alive():
        logger.warning("Consumer thread already running")
        return
    _stop.clear()
    _thread = threading.Thread(target=_run, daemon=True, name="kafka-consumer")
    _thread.start()
    logger.info("Consumer thread started")

def stop():
    _stop.set()
    logger.info("Consumer stop requested")
