# main.py
from fastapi import FastAPI
from app.version import VERSION
from app.api.v1 import routes_auth, routes_users
from prometheus_fastapi_instrumentator import Instrumentator

# Create instrumentator first
instrumentator = Instrumentator()

app = FastAPI(title='Auth Service', version=VERSION)

# Instrument the app BEFORE adding routes or middleware
instrumentator.instrument(app).expose(
    app,
    include_in_schema=False,
    endpoint="/auth/metrics",
    should_gzip=True,
)

# Add BOTH health endpoints for compatibility
@app.get('/health')
def health(): return {'status':'ok'}

@app.get('/auth/health')
def auth_health(): return {'status':'ok'}

@app.get('/v1/_info')
def info(): return {'service':'auth','version':VERSION}

# Debug: Print all routes on startup
@app.on_event("startup")
async def startup_event():
    for route in app.routes:
        if hasattr(route, "methods") and hasattr(route, "path"):
            print(f"{route.methods} {route.path}")

app.include_router(routes_auth.router, prefix='/auth', tags=['auth'])
app.include_router(routes_users.router, prefix='/users', tags=['users'])
