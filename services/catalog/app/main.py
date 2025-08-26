from fastapi import FastAPI
from app.version import VERSION
from app.api import products, categories
from app.api import inventory
from prometheus_fastapi_instrumentator import Instrumentator

# Create instrumentator first
instrumentator = Instrumentator()

app = FastAPI(title='Catalog Service', version=VERSION)

# Instrument the app BEFORE adding routes or middleware
instrumentator.instrument(app).expose(
    app,
    include_in_schema=False,
    endpoint="/catalog/metrics",  # Fixed: removed undefined SERVICE_PREFIX variable
    should_gzip=True,
)

@app.get('/health')
def health(): return {'status':'ok'}

@app.get('/catalog/health')
def auth_health(): return {'status':'ok'}

@app.get('/v1/_info')
def info(): return {'service':'catalog','version':VERSION}

# Debug: Print all routes on startup
@app.on_event("startup")
async def startup_event():
    for route in app.routes:
        if hasattr(route, "methods") and hasattr(route, "path"):
            print(f"{route.methods} {route.path}")

app.include_router(categories.router, prefix='/catalog/v1/categories', tags=['categories'])
app.include_router(products.router,   prefix='/catalog/v1/products',   tags=['products'])
app.include_router(inventory.router,  prefix='/catalog', tags=['inventory'])
