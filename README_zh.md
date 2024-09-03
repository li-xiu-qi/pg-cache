# PgCache 和 AsyncPgCache 使用说明

## 介绍

`PgCache` 和 `AsyncPgCache` 是用于 PostgreSQL 数据库的缓存管理类。`PgCache` 提供了同步操作，而 `AsyncPgCache`
提供了异步操作。它们允许您将数据缓存到 PostgreSQL 数据库中，并提供了设置、获取、删除和导入导出缓存的功能。

## 安装

在使用这些类之前，请确保您已经安装了以下 Python 包：

```bash
pip install sqlalchemy asyncpg
```

## 使用方法

### PgCache

#### 初始化

```python
from your_module import PgCache

db_url = "postgresql://user:password@localhost/dbname"
table_name = "cache_table"
cache = PgCache(db_url, table_name)
cache.init_db()
```

#### 设置缓存

```python
cache.set("my_key", "my_value", expire_after_seconds=3600)
```

#### 批量设置缓存

```python
entries = [
    {"key": "key1", "value": "value1"},
    {"key": "key2", "value": "value2"}
]
cache.set_bulk(entries, expire_after_seconds=3600)
```

#### 获取缓存

```python
value = cache.get("my_key")
```

#### 删除缓存

```python
cache.delete("my_key")
```

#### 批量删除缓存

```python
keys = ["key1", "key2"]
cache.delete_bulk(keys)
```

#### 清空缓存

```python
cache.flushdb()
```

#### 导出缓存到文件

```python
cache.export_to_file("cache_backup.json")
```

#### 从文件导入缓存

```python
cache.import_from_file("cache_backup.json")
```

### AsyncPgCache

#### 初始化

```python
import asyncio
from your_module import AsyncPgCache

db_url = "postgresql+asyncpg://user:password@localhost/dbname"
table_name = "cache_table"
cache = AsyncPgCache(db_url, table_name)


async def init():
    await cache.init_db()


asyncio.run(init())
```

#### 设置缓存

```python
async def set_cache():
    await cache.set("my_key", "my_value", expire_after_seconds=3600)


asyncio.run(set_cache())
```

#### 批量设置缓存

```python


entries

= [
    {"key": "key1", "value": "value1"},
    {"key": "key2", "value": "value2"}
]


async def set_bulk_cache():
    await cache.set_bulk(entries, expire_after_seconds=3600)


asyncio.run(set_bulk_cache())
```

#### 获取缓存

```python
async def get_cache():
    value = await cache.get("my_key")
    print(value)


asyncio.run(get_cache())
```

#### 删除缓存

```python
async def delete_cache():
    await cache.delete("my_key")


asyncio.run(delete_cache())
```

#### 批量删除缓存

```python
keys = ["key1", "key2"]


async def delete_bulk_cache():
    await cache.delete_bulk(keys)


asyncio.run(delete_bulk_cache())
```

#### 清空缓存

```python
async def flush_cache():
    await cache.flushdb()


asyncio.run(flush_cache())
```

#### 导出缓存到文件

```python
async def export_cache():
    await cache.export_to_file("cache_backup.json")


asyncio.run(export_cache())
```

#### 从文件导入缓存

```python
async def import_cache():
    await cache.import_from_file("cache_backup.json")


asyncio.run(import_cache())
```

## 日志

您可以通过在初始化 `PgCache` 或 `AsyncPgCache` 时设置 `log_level` 参数来控制日志级别。例如：

```python
cache = PgCache(db_url, table_name, log_level=logging.INFO)
```

## 结论

`PgCache` 和 `AsyncPgCache` 提供了强大的缓存管理功能，适用于需要将数据缓存到 PostgreSQL
数据库的应用程序。通过同步和异步两种方式，您可以根据需要选择合适的实现方式。