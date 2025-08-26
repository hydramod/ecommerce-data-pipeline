from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer,String,Text,Boolean,ForeignKey,BigInteger
from app.db.session import Base

class Category(Base):
    __tablename__='categories'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    products = relationship('Product', back_populates='category')

class Product(Base):
    __tablename__='products'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(240), nullable=False)
    description: Mapped[str] = mapped_column(Text, default='')
    price_cents: Mapped[int] = mapped_column(BigInteger, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), default='USD')
    sku: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    category_id: Mapped[int] = mapped_column(ForeignKey('categories.id'), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    category = relationship('Category', back_populates='products')
    images = relationship('ProductImage', back_populates='product', cascade='all, delete-orphan')
    inventory = relationship('Inventory', back_populates='product', uselist=False, cascade='all, delete-orphan')

class ProductImage(Base):
    __tablename__='product_images'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey('products.id', ondelete='CASCADE'))
    object_key: Mapped[str] = mapped_column(String(255), nullable=False)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)
    product = relationship('Product', back_populates='images')

class Inventory(Base):
    __tablename__='inventory'
    product_id: Mapped[int] = mapped_column(ForeignKey('products.id', ondelete='CASCADE'), primary_key=True)
    in_stock: Mapped[int] = mapped_column(Integer, default=0)
    reserved: Mapped[int] = mapped_column(Integer, default=0)
    product = relationship('Product', back_populates='inventory')
