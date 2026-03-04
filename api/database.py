"""
OnePilot – Database Layer
PostgreSQL via asyncpg + Redis cache
"""
from __future__ import annotations

import os
import json
import logging
from typing import Optional
from contextlib import asynccontextmanager

import asyncpg
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

# ── Config from env ────────────────────────────────────────────
PG_HOST     = os.getenv("METADATA_DB_HOST", "localhost")
PG_PORT     = int(os.getenv("METADATA_DB_PORT", "5433"))
PG_DB       = os.getenv("METADATA_DB_NAME", "onepilot_dev")
PG_USER     = os.getenv("METADATA_DB_USER", "onepilot")
PG_PASS     = os.getenv("METADATA_DB_PASSWORD", "onepilot_secret")

REDIS_HOST  = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT  = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB    = int(os.getenv("REDIS_DB", "0"))
REDIS_PASS  = os.getenv("REDIS_PASSWORD") or None

CACHE_TTL   = 300  # 5 minutes

# ── Singletons ────────────────────────────────────────────────
_pg_pool: Optional[asyncpg.Pool] = None
_redis:   Optional[aioredis.Redis] = None


async def get_pg_pool() -> asyncpg.Pool:
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = await asyncpg.create_pool(
            host=PG_HOST, port=PG_PORT,
            database=PG_DB, user=PG_USER, password=PG_PASS,
            min_size=2, max_size=10,
            command_timeout=60,
        )
        logger.info(f"[DB] Pool PostgreSQL connecté ({PG_HOST}:{PG_PORT}/{PG_DB})")
    return _pg_pool


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.Redis(
            host=REDIS_HOST, port=REDIS_PORT,
            db=REDIS_DB, password=REDIS_PASS,
            decode_responses=True,
        )
        await _redis.ping()
        logger.info(f"[Redis] Connecté ({REDIS_HOST}:{REDIS_PORT})")
    return _redis


async def close_connections():
    global _pg_pool, _redis
    if _pg_pool:
        await _pg_pool.close()
        _pg_pool = None
    if _redis:
        await _redis.close()
        _redis = None


@asynccontextmanager
async def pg_conn():
    pool = await get_pg_pool()
    async with pool.acquire() as conn:
        yield conn


# ── Cache helpers ─────────────────────────────────────────────
async def cache_set(key: str, value: dict, ttl: int = CACHE_TTL):
    try:
        r = await get_redis()
        await r.setex(f"onepilot:{key}", ttl, json.dumps(value))
    except Exception as e:
        logger.warning(f"[Cache] set failed: {e}")


async def cache_get(key: str) -> Optional[dict]:
    try:
        r = await get_redis()
        raw = await r.get(f"onepilot:{key}")
        return json.loads(raw) if raw else None
    except Exception as e:
        logger.warning(f"[Cache] get failed: {e}")
        return None


async def cache_invalidate(pattern: str):
    try:
        r = await get_redis()
        keys = await r.keys(f"onepilot:{pattern}*")
        if keys:
            await r.delete(*keys)
    except Exception as e:
        logger.warning(f"[Cache] invalidate failed: {e}")


# ── Init schema ───────────────────────────────────────────────
async def init_schema():
    """Exécute la migration SQL si les tables n'existent pas."""
    migration_file = os.path.join(
        os.path.dirname(__file__), "..", "db", "migrations", "001_init.sql"
    )
    if not os.path.exists(migration_file):
        logger.warning("[DB] Fichier migration introuvable, skip.")
        return
    with open(migration_file) as f:
        sql = f.read()
    async with pg_conn() as conn:
        await conn.execute(sql)
    logger.info("[DB] Schema initialisé.")