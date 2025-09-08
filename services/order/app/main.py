from fastapi import FastAPI
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from app.version import VERSION
from app.api import routes
from app.kafka import consumer as payment_consumer
from app.kafka.producer import check_kafka_health
from app.db.session import check_db_health
from app.api.routes import check_redis_health
from prometheus_fastapi_instrumentator import Instrumentator
import logging
from contextlib import asynccontextmanager
from app.core.limiting import limiter  # shared limiter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Order Service")

    # Start Kafka consumer (payments -> mark order PAID)
    payment_consumer.start()

    yield

    logger.info("Shutting down Order Service")
    payment_consumer.stop()

instrumentator = Instrumentator()

app = FastAPI(
    title="Order Service",
    version=VERSION,
    lifespan=lifespan,
)

# Rate limit exception handler uses the shared limiter
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Metrics
instrumentator.instrument(app).expose(
    app,
    include_in_schema=False,
    endpoint="/order/metrics",
    should_gzip=True,
)

# Health endpoints
@app.get("/health")
def health():
    db_healthy = check_db_health()
    redis_healthy = check_redis_health()
    kafka_healthy = check_kafka_health()
    status = "healthy" if all([db_healthy, redis_healthy, kafka_healthy]) else "unhealthy"
    return {
        "status": status,
        "services": {
            "database": db_healthy,
            "redis": redis_healthy,
            "kafka": kafka_healthy,
        },
    }

@app.get("/order/health")
def order_health():
    return {"status": "ok"}

@app.get("/v1/_info")
def info():
    return {"service": "order", "version": VERSION}

# Routes
app.include_router(routes.router, prefix="/order", tags=["orders"])
