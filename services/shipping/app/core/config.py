from pydantic import BaseModel
import os

class Settings(BaseModel):
    POSTGRES_DSN: str = os.getenv("POSTGRES_DSN", "postgresql+psycopg://postgres:postgres@postgres:5432/appdb")
    KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    TOPIC_ORDER_EVENTS: str = os.getenv("TOPIC_ORDER_EVENTS", "order.events")
    TOPIC_PAYMENT_EVENTS: str = os.getenv("TOPIC_PAYMENT_EVENTS", "payment.events")
    TOPIC_SHIPPING_EVENTS: str = os.getenv("TOPIC_SHIPPING_EVENTS", "shipping.events")

settings = Settings()
