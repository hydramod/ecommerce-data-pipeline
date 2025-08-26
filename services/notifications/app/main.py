from fastapi import FastAPI
from app.api.routes import router
from app.kafka import consumer
from prometheus_fastapi_instrumentator import Instrumentator

# Create instrumentator first
instrumentator = Instrumentator()

app = FastAPI(title="Notifications Service")

# Instrument the app BEFORE adding routes or middleware
instrumentator.instrument(app).expose(
    app,
    include_in_schema=False,
    endpoint="/notifications/metrics",  # Fixed: removed undefined variable
    should_gzip=True,
)

@app.on_event("startup")
async def startup():
    consumer.start()

@app.on_event("shutdown")
async def shutdown():
    consumer.stop()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get('/notifications/health')
def auth_health(): return {'status':'ok'}

# Debug: Print all routes on startup
@app.on_event("startup")
async def startup_event():
    for route in app.routes:
        if hasattr(route, "methods") and hasattr(route, "path"):
            print(f"{route.methods} {route.path}")

app.include_router(router)
