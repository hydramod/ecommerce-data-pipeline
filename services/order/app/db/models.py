from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, DateTime, ForeignKey, BigInteger, text, Index
from datetime import datetime
from app.db.session import Base

class Order(Base):
    __tablename__ = "orders"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_email: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default="CREATED")
    total_cents: Mapped[int] = mapped_column(BigInteger, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, server_default="USD")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=text("(now() at time zone 'utc')"))
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=text("(now() at time zone 'utc')"), onupdate=text("(now() at time zone 'utc')"))

    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan", lazy="selectin")

class OrderItem(Base):
    __tablename__ = "order_items"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)
    product_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    qty: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price_cents: Mapped[int] = mapped_column(BigInteger, nullable=False)
    title_snapshot: Mapped[str] = mapped_column(String(255), nullable=False)

    order = relationship("Order", back_populates="items")

# Create composite indexes
Index('ix_order_status_user', Order.status, Order.user_email)
Index('ix_order_item_product', OrderItem.product_id, OrderItem.order_id)