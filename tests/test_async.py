import logging
from pg_cache.async_cache import AsyncPgCache
import asyncio

DATABASE_URL = "postgresql+asyncpg://ke:2003730Li@localhost:5432/using_test"

async def main():
    cache = AsyncPgCache(DATABASE_URL, "cache_table", log_level=logging.ERROR)

    # 初始化数据库
    await cache.init_db()

    # 设置缓存
    await cache.set("test_key", {"foo": "bar"}, expire_after_seconds=60, partition_key="partition1")
    print("Set cache entry")

    # 获取缓存
    value = await cache.get("test_key", partition_key="partition1")
    print(f"Got cache entry: {value}")

    # 删除缓存
    await cache.delete("test_key", partition_key="partition1")
    print("Deleted cache entry")

    # 获取已删除的缓存
    value = await cache.get("test_key", partition_key="partition1")
    print(f"Got cache entry after deletion: {value}")

if __name__ == "__main__":
    asyncio.run(main())