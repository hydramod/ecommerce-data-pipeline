from pydantic import BaseModel
import os

class Settings(BaseModel):
    KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    # Missing payment-specific security settings
    JWT_SECRET: str = os.getenv("JWT_SECRET") or ''
    SVC_INTERNAL_KEY: str = os.getenv("SVC_INTERNAL_KEY") or ''
    # Add payment gateway settings (Stripe, etc.)
    STRIPE_SECRET_KEY: str = os.getenv("STRIPE_SECRET_KEY") or ''
    STRIPE_WEBHOOK_SECRET: str = os.getenv("STRIPE_WEBHOOK_SECRET") or ''

# Validate required security settings
if not all([os.getenv('JWT_SECRET'), os.getenv('SVC_INTERNAL_KEY'), os.getenv('STRIPE_SECRET_KEY')]):
    import warnings
    warnings.warn("Missing required environment variables for security")

settings = Settings()