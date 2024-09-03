import asyncio
import logging
import os

from pg_cache import AsyncPgCache, PgCache
from find_env import find_project_root_and_load_dotenv

find_project_root_and_load_dotenv("pg-cache")

DATABASE_URL = os.getenv("DATABASE_URL")


def test_sync_pg_cache():
    # 初始化同步缓存
    sync_cache = PgCache(DATABASE_URL.replace("+asyncpg", ""), "sync_cache", log_level=logging.INFO)
    sync_cache.init_db()

    # 测试 set 和 get
    sync_cache.set("test_key", {"data": "test_value"}, expire_after_seconds=60)
    value = sync_cache.get("test_key")
    print(f"Sync get: {value}")

    # 测试 delete
    sync_cache.delete("test_key")
    value = sync_cache.get("test_key")
    print(f"Sync get after delete: {value}")

    # 测试 set_bulk 和 delete_bulk
    sync_cache.set_bulk([{"key": "key1", "value": "value1"}, {"key": "key2", "value": "value2"}],
                        expire_after_seconds=60)
    value1 = sync_cache.get("key1")
    value2 = sync_cache.get("key2")
    print(f"Sync get bulk: {value1}, {value2}")

    sync_cache.delete_bulk(["key1", "key2"])
    value1 = sync_cache.get("key1")
    value2 = sync_cache.get("key2")
    print(f"Sync get bulk after delete: {value1}, {value2}")

    # 测试 flushdb
    sync_cache.set("test_key", {"data": "test_value"}, expire_after_seconds=60)
    sync_cache.flushdb()
    value = sync_cache.get("test_key")
    print(f"Sync get after flushdb: {value}")


async def test_async_pg_cache():
    # 初始化异步缓存
    async_cache = AsyncPgCache(DATABASE_URL, "async_cache", log_level=logging.INFO)
    await async_cache.init_db()

    # 测试 set 和 get
    await async_cache.set("test_key", {"data": "test_value"}, expire_after_seconds=60)
    value = await async_cache.get("test_key")
    print(f"Async get: {value}")

    # 测试 delete
    await async_cache.delete("test_key")
    value = await async_cache.get("test_key")
    print(f"Async get after delete: {value}")

    # 测试 set_bulk 和 delete_bulk
    await async_cache.set_bulk([{"key": "key1", "value": "value1"}, {"key": "key2", "value": "value2"}],
                               expire_after_seconds=60)
    value1 = await async_cache.get("key1")
    value2 = await async_cache.get("key2")
    print(f"Async get bulk: {value1}, {value2}")

    await async_cache.delete_bulk(["key1", "key2"])
    value1 = await async_cache.get("key1")
    value2 = await async_cache.get("key2")
    print(f"Async get bulk after delete: {value1}, {value2}")

    # 测试 flushdb
    await async_cache.set("test_key", {"data": "test_value"}, expire_after_seconds=60)
    await async_cache.flushdb()
    value = await async_cache.get("test_key")
    print(f"Async get after flushdb: {value}")


if __name__ == "__main__":
    test_sync_pg_cache()
    asyncio.run(test_async_pg_cache())
