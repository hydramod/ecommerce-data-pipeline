from fastapi import FastAPI, Depends
from app.version import VERSION
from app.api.routes import router as shipping_router
from app.kafka import consumer as shipping_consumer
from app.kafka.producer import close_producer
from app.db.session import check_db_health
from app.kafka.producer import _get_producer
from prometheus_fastapi_instrumentator import Instrumentator
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Shipping Service")
    
    # Print all routes for debugging
    for route in app.routes:
        if hasattr(route, "methods") and hasattr(route, "path"):
            logger.info(f"{route.methods} {route.path}")
    
    # Start Kafka consumer
    shipping_consumer.start()
    
    yield
    
    # Shutdown
    logger.info("Shutting down Shipping Service")
    shipping_consumer.stop()
    close_producer()

app = FastAPI(
    title="Shipping Service", 
    version=VERSION,
    lifespan=lifespan
)

# Create instrumentator
instrumentator = Instrumentator()

# Instrument the app
instrumentator.instrument(app).expose(
    app,
    include_in_schema=False,
    endpoint="/shipping/metrics",  # Standardized metrics endpoint
    should_gzip=True,
)

def check_kafka_health():
    """Check Kafka connection health"""
    try:
        producer = _get_producer()
        # Try to get metadata to verify connection
        producer.list_topics(timeout=5)
        return True
    except Exception as e:
        logger.error(f"Kafka health check failed: {e}")
        return False

@app.get("/health")
def health():
    """Comprehensive health check including dependencies"""
    db_healthy = check_db_health()
    kafka_healthy = check_kafka_health()
    
    status = "healthy" if all([db_healthy, kafka_healthy]) else "unhealthy"
    return {
        "status": status,
        "services": {
            "database": db_healthy,
            "kafka": kafka_healthy
        }
    }

@app.get("/shipping/health")
def shipping_health():
    """Simple health check for load balancers"""
    return {"status": "ok"}

@app.get("/v1/_info")
def info():
    return {"service": "shipping", "version": VERSION}

# Include routers
app.include_router(shipping_router)