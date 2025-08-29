from fastapi import APIRouter, Depends, HTTPException, Header, status
from pydantic import BaseModel, EmailStr, Field, validator
from sqlalchemy.orm import Session
from typing import Optional, List
import secrets
import logging
from app.db.session import SessionLocal, get_db
from app.db.models import Shipment, ShipmentStatus
from app.kafka.producer import emit as emit_shipping_event
from app.core.config import settings
import jwt
from datetime import datetime

logger = logging.getLogger(__name__)
router = APIRouter()

def verify_internal_request(x_internal_key: str = Header(...)):
    """Verify internal service requests"""
    if not settings.SVC_INTERNAL_KEY:
        raise HTTPException(status_code=500, detail="Service not configured")
    if x_internal_key != settings.SVC_INTERNAL_KEY:
        raise HTTPException(status_code=401, detail="Invalid internal key")

class CreateShipment(BaseModel):
    order_id: int = Field(..., gt=0)
    user_email: EmailStr
    address_line1: str = Field(..., min_length=1, max_length=255)
    address_line2: Optional[str] = Field("", max_length=255)
    city: str = Field(..., min_length=1, max_length=120)
    country: str = Field(..., min_length=2, max_length=2)
    postcode: str = Field(..., min_length=1, max_length=32)
    
    @validator('country')
    def country_uppercase(cls, v):
        return v.upper()

class ShipmentOut(BaseModel):
    id: int
    order_id: int
    user_email: EmailStr
    address_line1: str
    address_line2: Optional[str] = ""
    city: str
    country: str
    postcode: str
    carrier: Optional[str] = ""
    tracking_number: Optional[str] = ""
    status: ShipmentStatus
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

@router.post("/shipping/v1/shipments", response_model=ShipmentOut, status_code=201)
def create_shipment(payload: CreateShipment, db: Session = Depends(get_db), x_internal_key: str = Header(...)):
    # Verify internal request
    verify_internal_request(x_internal_key)
    
    # Check if shipment already exists for this order
    existing = db.query(Shipment).filter(Shipment.order_id == payload.order_id).first()
    if existing:
        raise HTTPException(status_code=409, detail="Shipment already exists for this order")
    
    try:
        shp = Shipment(
            order_id=payload.order_id,
            user_email=payload.user_email,
            address_line1=payload.address_line1,
            address_line2=payload.address_line2 or "",
            city=payload.city,
            country=payload.country.upper(),
            postcode=payload.postcode,
            status=ShipmentStatus.PENDING_PAYMENT,
        )
        db.add(shp)
        db.commit()
        db.refresh(shp)
        return ShipmentOut.model_validate(shp)
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to create shipment: {e}")
        raise HTTPException(status_code=500, detail="Failed to create shipment")

@router.get("/shipping/v1/shipments/{shipment_id}", response_model=ShipmentOut)
def get_shipment(shipment_id: int, db: Session = Depends(get_db)):
    shp = db.get(Shipment, shipment_id)
    if not shp:
        raise HTTPException(404, "Shipment not found")
    return ShipmentOut.model_validate(shp)

@router.get("/shipping/v1/shipments", response_model=List[ShipmentOut])
def list_shipments(order_id: Optional[int] = None, db: Session = Depends(get_db)):
    try:
        q = db.query(Shipment)
        if order_id is not None:
            q = q.filter(Shipment.order_id == order_id)
        rows = q.all()
        return [ShipmentOut.model_validate(r) for r in rows]
    except Exception as e:
        logger.error(f"Failed to list shipments: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve shipments")

@router.post("/shipping/v1/shipments/{shipment_id}/dispatch", response_model=ShipmentOut)
def dispatch_shipment(shipment_id: int, db: Session = Depends(get_db)):
    shp = db.get(Shipment, shipment_id)
    if not shp:
        raise HTTPException(404, "Shipment not found")
    if shp.status != ShipmentStatus.READY_TO_SHIP:
        raise HTTPException(409, f"Shipment not ready to ship (status={shp.status})")
    
    try:
        shp.carrier = "DemoCarrier"
        shp.tracking_number = secrets.token_hex(6).upper()
        shp.status = ShipmentStatus.DISPATCHED
        db.add(shp)
        db.commit()
        db.refresh(shp)

        emit_shipping_event({
            "type": "shipping.dispatched",
            "order_id": shp.order_id,
            "user_email": shp.user_email,
            "shipment_id": shp.id,
            "tracking_number": shp.tracking_number
        })

        return ShipmentOut.model_validate(shp)
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to dispatch shipment: {e}")
        raise HTTPException(status_code=500, detail="Failed to dispatch shipment")