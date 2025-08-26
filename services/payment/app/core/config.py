
from pydantic import BaseModel
import os

class Settings(BaseModel):
    KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

settings = Settings()
