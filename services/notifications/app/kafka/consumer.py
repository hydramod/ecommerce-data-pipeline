import json, threading, smtplib
from email.mime.text import MIMEText
from kafka import KafkaConsumer
from app.core.config import settings

_stop = threading.Event()
_thread = None

# Cache order_id -> user_email learned from order.created
_order_email = {}

def send_email(to: str, subject: str, body: str):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = settings.FROM_EMAIL
    msg["To"] = to
    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as s:
        s.sendmail(settings.FROM_EMAIL, [to], msg.as_string())

def _handle(ev: dict):
    t = ev.get("type")
    if t == "order.created":
        if ev.get("order_id") and ev.get("user_email"):
            _order_email[ev["order_id"]] = ev["user_email"]
        # Email: order received
        if ev.get("user_email"):
            send_email(ev["user_email"], "Order received",
                       f"We received your order {ev['order_id']} for {ev.get('amount_cents','?')} cents.")
    elif t == "payment.succeeded":
        email = ev.get("user_email") or _order_email.get(ev.get("order_id"))
        if email:
            send_email(email, "Payment received",
                       f"Payment for order {ev['order_id']} succeeded.")
    elif t == "shipping.ready":
        email = ev.get("user_email") or _order_email.get(ev.get("order_id"))
        if email:
            send_email(email, "Order ready to ship",
                       f"Your order {ev['order_id']} is ready to ship.")
    elif t == "shipping.dispatched":
        email = ev.get("user_email") or _order_email.get(ev.get("order_id"))
        if email:
            send_email(email, "Order dispatched",
                       f"Your order {ev['order_id']} has been dispatched. "
                       f"Tracking: {ev.get('tracking_number','TBA')}")

def _run():
    consumer = KafkaConsumer(
        settings.TOPIC_ORDER_EVENTS,
        settings.TOPIC_PAYMENT_EVENTS,
        settings.TOPIC_SHIPPING_EVENTS,
        bootstrap_servers=[settings.KAFKA_BOOTSTRAP],
        group_id="notifications-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    try:
        for msg in consumer:
            if _stop.is_set():
                break
            _handle(msg.value)
    finally:
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
