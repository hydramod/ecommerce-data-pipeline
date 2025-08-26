from fastapi import FastAPI
from app.version import VERSION
from app.api import routes as cart_routes
from prometheus_fastapi_instrumentator import Instrumentator

# Create instrumentator first
instrumentator = Instrumentator()

app = FastAPI(title="Cart Service", version=VERSION)

# Instrument the app BEFORE adding routes or middleware
instrumentator.instrument(app).expose(
    app,
    include_in_schema=False,
    endpoint="/cart/metrics",
    should_gzip=True,
)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get('/cart/health')
def auth_health(): return {'status':'ok'}

@app.get("/v1/_info")
def info():
    return {"service": "cart", "version": VERSION}

# Debug: Print all routes on startup
@app.on_event("startup")
async def startup_event():
    for route in app.routes:
        if hasattr(route, "methods") and hasattr(route, "path"):
            print(f"{route.methods} {route.path}")

app.include_router(cart_routes.router, prefix='/cart', tags=['cart'])
