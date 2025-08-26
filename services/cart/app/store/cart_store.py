
import json
from typing import Dict, Any
from redis import Redis
from app.core.config import settings

def get_client() -> Redis:
    return Redis.from_url(settings.REDIS_URL, decode_responses=True)

def cart_key(email: str) -> str:
    return f"cart:{email}"

def get_cart(email: str) -> Dict[str, Any]:
    r = get_client()
    key = cart_key(email)
    items = r.hgetall(key)  # {product_id_str: json}
    parsed = {}
    for pid, val in items.items():
        try:
            parsed[pid] = json.loads(val)
        except Exception:
            continue
    return {"items": list(parsed.values())}

def put_item(email: str, item: Dict[str, Any]):
    r = get_client()
    key = cart_key(email)
    pid = str(item["product_id"])
    r.hset(key, pid, json.dumps(item))

def delete_item(email: str, product_id: int):
    r = get_client()
    r.hdel(cart_key(email), str(product_id))

def clear_cart(email: str):
    r = get_client()
    r.delete(cart_key(email))
