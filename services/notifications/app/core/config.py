from pydantic import BaseModel
import os

class Settings(BaseModel):
    KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    TOPIC_ORDER_EVENTS: str = os.getenv("TOPIC_ORDER_EVENTS", "order.events")
    TOPIC_PAYMENT_EVENTS: str = os.getenv("TOPIC_PAYMENT_EVENTS", "payment.events")
    TOPIC_SHIPPING_EVENTS: str = os.getenv("TOPIC_SHIPPING_EVENTS", "shipping.events")
    SMTP_HOST: str = os.getenv("SMTP_HOST", "mailhog")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "1025"))
    FROM_EMAIL: str = os.getenv("FROM_EMAIL", "no-reply@example.local")

settings = Settings()
