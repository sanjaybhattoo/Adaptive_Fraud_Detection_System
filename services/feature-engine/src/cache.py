import logging
from typing import Optional, Set, Any

import redis
logger = logging.getLogger(__name__)

class RedisCache:
    
    def __init__(self, redis_url: str):
        try:
            self.redis = redis.from_url(redis_url, decode_responses=True)
            self.redis.ping()
            logger.info("connected")
        except Exception as e:
            logger.error(f"connection failed: {e}")
            raise
    
    def get(self, key: str) -> Optional[str]:
        try:
            return self.redis.get(key)
        except Exception as e:
            logger.error(f"Failed  cache: {e}")
            return None
    
    def get_int(self, key: str) -> Optional[int]:
        """Get integer value from cache"""
        try:
            val = self.redis.get(key)
            return int(val) if val else None
        except Exception as e:
            logger.error(f"Failed  cache: {e}")
            return None
    
    def set(self, key: str, value: Any, expire: int = None) -> bool:
        try:
            self.redis.set(key, value, ex=expire)
            return True
        except Exception as e:
            logger.error(f"Failed e: {e}")
            return False
    
    def increment(self, key: str, amount: int = 1, expire: int = None) -> Optional[int]:
        try:
            val = self.redis.incr(key, amount)
            if expire:
                self.redis.expire(key, expire)
            return val
        except Exception as e:
            logger.error(f"Failed increment: {e}")
            return None
    
    def add_to_set(self, key: str, value: Any, expire: int = None) -> bool:
        try:
            self.redis.sadd(key, value)
            if expire:
                self.redis.expire(key, expire)
            return True
        except Exception as e:
            logger.error(f"Faile set: {e}")
            return False
    
    def get_set_size(self, key: str) -> int:
        try:
            return self.redis.scard(key)
        except Exception as e:
            logger.error(f"Failed size: {e}")
            return 0
    
    def delete(self, key: str) -> bool:
        try:
            self.redis.delete(key)
            return True
        except Exception as e:
            logger.error(f"Failed key: {e}")
            return False
    
    def close(self):
        try:
            self.redis.close()
            logger.info("Redis closed")
        except Exception as e:
            logger.error(f"Failed Redis: {e}")
