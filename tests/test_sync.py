import logging
from pg_cache.sync_cache import PgCache

DATABASE_URL = "postgresql://ke:2003730Li@localhost:5432/using_test"

def main():
    cache = PgCache(DATABASE_URL, "cache_table", log_level=logging.ERROR)

    # 初始化数据库
    cache.init_db()

    # 设置缓存
    cache.set("test_key", {"foo": "bar"}, expire_after_seconds=60, partition_key="partition1")
    print("Set cache entry")

    # 获取缓存
    value = cache.get("test_key", partition_key="partition1")
    print(f"Got cache entry: {value}")

    # 删除缓存
    cache.delete("test_key", partition_key="partition1")
    print("Deleted cache entry")

    # 获取已删除的缓存
    value = cache.get("test_key", partition_key="partition1")
    print(f"Got cache entry after deletion: {value}")

if __name__ == "__main__":
    main()