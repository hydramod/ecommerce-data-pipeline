from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String, DateTime, Enum as SAEnum
from datetime import datetime
from enum import Enum
from app.db.session import Base

class ShipmentStatus(str, Enum):
    PENDING_PAYMENT = "PENDING_PAYMENT"
    READY_TO_SHIP = "READY_TO_SHIP"
    DISPATCHED = "DISPATCHED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"

class Shipment(Base):
    __tablename__ = "shipments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(Integer, index=True)
    user_email: Mapped[str] = mapped_column(String(255))
    address_line1: Mapped[str] = mapped_column(String(255))
    address_line2: Mapped[str] = mapped_column(String(255), default="")
    city: Mapped[str] = mapped_column(String(120))
    country: Mapped[str] = mapped_column(String(2))  # ISO2
    postcode: Mapped[str] = mapped_column(String(32))
    carrier: Mapped[str] = mapped_column(String(64), default="")
    tracking_number: Mapped[str] = mapped_column(String(64), default="")
    status: Mapped[str] = mapped_column(SAEnum(ShipmentStatus), default=ShipmentStatus.PENDING_PAYMENT)
    created_at: Mapped[datetime] = mapped_column(DateTime(), default=lambda: datetime.utcnow())
    updated_at: Mapped[datetime] = mapped_column(DateTime(), default=lambda: datetime.utcnow())
