from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.orm import Session
from typing import Optional, List
import secrets

from app.db.session import SessionLocal
from app.db.models import Shipment, ShipmentStatus
from app.kafka.producer import emit as emit_shipping_event

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class CreateShipment(BaseModel):
    order_id: int
    user_email: EmailStr
    address_line1: str
    address_line2: Optional[str] = ""
    city: str
    country: str = Field(min_length=2, max_length=2)
    postcode: str

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

@router.post("/shipping/v1/shipments", response_model=ShipmentOut, status_code=201)
def create_shipment(payload: CreateShipment, db: Session = Depends(get_db)):
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
    db.add(shp); db.commit(); db.refresh(shp)
    return ShipmentOut(**shp.__dict__)

@router.get("/shipping/v1/shipments/{shipment_id}", response_model=ShipmentOut)
def get_shipment(shipment_id: int, db: Session = Depends(get_db)):
    shp = db.get(Shipment, shipment_id)
    if not shp:
        raise HTTPException(404, "Not found")
    return ShipmentOut(**shp.__dict__)

@router.get("/shipping/v1/shipments", response_model=List[ShipmentOut])
def list_shipments(order_id: Optional[int] = None, db: Session = Depends(get_db)):
    q = db.query(Shipment)
    if order_id is not None:
        q = q.filter(Shipment.order_id == int(order_id))
    rows = q.all()
    return [ShipmentOut(**r.__dict__) for r in rows]

@router.post("/shipping/v1/shipments/{shipment_id}/dispatch", response_model=ShipmentOut)
def dispatch_shipment(shipment_id: int, db: Session = Depends(get_db)):
    shp = db.get(Shipment, shipment_id)
    if not shp:
        raise HTTPException(404, "Not found")
    if shp.status != ShipmentStatus.READY_TO_SHIP:
        raise HTTPException(409, f"Shipment not ready to ship (status={shp.status})")
    shp.carrier = "DemoCarrier"
    shp.tracking_number = secrets.token_hex(6).upper()
    shp.status = ShipmentStatus.DISPATCHED
    db.add(shp); db.commit(); db.refresh(shp)

    emit_shipping_event({
        "type": "shipping.dispatched",
        "order_id": shp.order_id,
        "user_email": shp.user_email,
        "shipment_id": shp.id,
        "tracking_number": shp.tracking_number
    })

    return ShipmentOut(**shp.__dict__)
