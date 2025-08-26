from fastapi import FastAPI
from app.version import VERSION
from app.api.routes import router as shipping_router
from app.kafka import consumer as shipping_consumer
from prometheus_fastapi_instrumentator import Instrumentator

# Create instrumentator first
instrumentator = Instrumentator()

app = FastAPI(title="Shipping Service", version=VERSION)

# Instrument the app BEFORE adding routes or middleware
instrumentator.instrument(app).expose(
    app,
    include_in_schema=False,
    endpoint="/shipping/metrics",  # Fixed: removed undefined SERVICE_PREFIX variable
    should_gzip=True,
)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/shipping/health")
def auth_health():
    return {"status": "ok"}

@app.get("/v1/_info")
def info():
    return {"service": "shipping", "version": VERSION}

app.include_router(shipping_router)

@app.on_event("startup")
async def startup_event():
    shipping_consumer.start()

@app.on_event("shutdown")
async def shutdown_event():
    shipping_consumer.stop()

@app.on_event("startup")
async def startup_event():
    # Print all routes for debugging
    for route in app.routes:
        if hasattr(route, "methods") and hasattr(route, "path"):
            print(f"{route.methods} {route.path}")
