from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer,String,DateTime,Boolean,ForeignKey
from datetime import datetime
from app.db.session import Base

class User(Base):
    __tablename__='users'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(32), default='customer')
    created_at: Mapped[datetime] = mapped_column(DateTime(), default=lambda: datetime.utcnow())
    updated_at: Mapped[datetime] = mapped_column(DateTime(), default=lambda: datetime.utcnow())
    refresh_tokens = relationship('RefreshToken', back_populates='user', cascade='all, delete-orphan')

class RefreshToken(Base):
    __tablename__='refresh_tokens'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey('users.id', ondelete='CASCADE'))
    jti: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(), nullable=False)
    revoked: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(), default=lambda: datetime.utcnow())
    user = relationship('User', back_populates='refresh_tokens')
