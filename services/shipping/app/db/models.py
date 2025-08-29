from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String, DateTime, Enum as SAEnum, text
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
    order_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    user_email: Mapped[str] = mapped_column(String(255), nullable=False)
    address_line1: Mapped[str] = mapped_column(String(255), nullable=False)
    address_line2: Mapped[str] = mapped_column(String(255), nullable=True, default="")
    city: Mapped[str] = mapped_column(String(120), nullable=False)
    country: Mapped[str] = mapped_column(String(2), nullable=False)  # ISO2
    postcode: Mapped[str] = mapped_column(String(32), nullable=False)
    carrier: Mapped[str] = mapped_column(String(64), nullable=True, default="")
    tracking_number: Mapped[str] = mapped_column(String(64), nullable=True, default="")
    status: Mapped[str] = mapped_column(SAEnum(ShipmentStatus), nullable=False, default=ShipmentStatus.PENDING_PAYMENT)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=text("(now() at time zone 'utc')"))
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=text("(now() at time zone 'utc')"), onupdate=text("(now() at time zone 'utc')"))