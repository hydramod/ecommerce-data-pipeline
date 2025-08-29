import json
from typing import Dict, Any
import redis
from functools import lru_cache
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

@lru_cache(maxsize=None)
def get_redis_pool():
    return redis.ConnectionPool.from_url(settings.REDIS_URL, decode_responses=True)

def get_client() -> redis.Redis:
    pool = get_redis_pool()
    return redis.Redis(connection_pool=pool)

def cart_key(email: str) -> str:
    return f"cart:{email}"

def get_cart(email: str) -> Dict[str, Any]:
    r = get_client()
    key = cart_key(email)
    
    product_ids = r.hkeys(key)
    if not product_ids:
        return {"items": []}
    
    with r.pipeline() as pipe:
        for pid in product_ids:
            pipe.hget(key, pid)
        items_json = pipe.execute()
    
    items = []
    for item_json in items_json:
        if item_json:
            try:
                items.append(json.loads(item_json))
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse cart item JSON for user {email}")
                continue
    
    return {"items": items}

def put_item(email: str, item: Dict[str, Any]):
    r = get_client()
    key = cart_key(email)
    pid = str(item["product_id"])
    
    with r.pipeline() as pipe:
        pipe.hset(key, pid, json.dumps(item))
        pipe.expire(key, 86400 * 7)  # 7 days expiration
        pipe.execute()

def delete_item(email: str, product_id: int):
    r = get_client()
    r.hdel(cart_key(email), str(product_id))

def clear_cart(email: str):
    r = get_client()
    r.delete(cart_key(email))