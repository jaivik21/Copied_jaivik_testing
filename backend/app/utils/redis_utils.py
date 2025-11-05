# get_redis(), TTL management, key generation

import redis.asyncio as aioredis
import os
from dotenv import load_dotenv

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_redis = None

def get_redis():
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(REDIS_URL, decode_responses=False)
    return _redis

def _key(session_id: str) -> str:
    return f"session:{session_id}:chunks"

async def create_session(session_id: str):
    redis = get_redis()
    key = _key(session_id)
    await redis.delete(key)
    await redis.expire(key, 3600)  

async def add_audio_chunk(session_id: str, chunk_bytes: bytes):
    redis = get_redis()
    key = _key(session_id)
    await redis.rpush(key, chunk_bytes)
    await redis.expire(key, 3600)

async def get_audio_chunks(session_id: str):
    redis = get_redis()
    return await redis.lrange(_key(session_id), 0, -1) or []

async def remove_session(session_id: str):
    await get_redis().delete(_key(session_id))

async def close_redis():
    global _redis
    if _redis:
        await _redis.close()
        _redis = None

def _meta_key(session_id: str) -> str:
    return f"session:{session_id}:meta"

async def set_session_meta(session_id: str, data: dict):
    redis = get_redis()
    key = _meta_key(session_id)
    pipe = redis.pipeline()
    for k, v in (data or {}).items():
        bval = v if isinstance(v, (bytes, bytearray)) else str(v).encode("utf-8")
        pipe.hset(key, k, bval)
    pipe.expire(key, 7200)
    await pipe.execute()

async def get_session_meta(session_id: str) -> dict:
    redis = get_redis()
    key = _meta_key(session_id)
    raw = await redis.hgetall(key)
    if not raw:
        return {}
    return { (k.decode("utf-8") if isinstance(k, (bytes, bytearray)) else str(k)):
             (v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else str(v))
             for k, v in raw.items() }

async def delete_session_all(session_id: str):
    redis = get_redis()
    await redis.delete(_meta_key(session_id))
    await remove_session(session_id)