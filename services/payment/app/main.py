from fastapi import FastAPI, Depends
from app.version import VERSION
from app.api import routes
from app.kafka.producer import get_producer
from prometheus_fastapi_instrumentator import Instrumentator
import logging
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Payment Service")
    
    # Print all routes for debugging
    for route in app.routes:
        if hasattr(route, "methods") and hasattr(route, "path"):
            logger.info(f"{route.methods} {route.path}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Payment Service")
    try:
        producer = get_producer()
        producer.flush(timeout=5)
        producer.close()
    except Exception as e:
        logger.error(f"Error closing Kafka producer: {e}")

app = FastAPI(
    title="Payment Service", 
    version=VERSION,
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Instrument the app
instrumentator = Instrumentator()
instrumentator.instrument(app).expose(
    app,
    include_in_schema=False,
    endpoint="/payment/metrics",
    should_gzip=True,
)

def check_kafka_health():
    """Check Kafka connection health"""
    try:
        producer = get_producer()
        # Try to get metadata to verify connection
        producer.list_topics(timeout=5)
        return True
    except Exception as e:
        logger.error(f"Kafka health check failed: {e}")
        return False

@app.get("/health")
def health():
    kafka_healthy = check_kafka_health()
    status = "healthy" if kafka_healthy else "unhealthy"
    return {
        "status": status,
        "services": {
            "kafka": kafka_healthy
        }
    }

@app.get('/payment/health')
def payment_health():
    return {'status':'ok'}

@app.get("/v1/_info")
def info():
    return {"service":"payment","version":VERSION}

app.include_router(routes.router, prefix='/payment', tags=["payments"])