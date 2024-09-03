# PgCache and AsyncPgCache Usage Guide

## Introduction

`PgCache` and `AsyncPgCache` are cache management classes for PostgreSQL databases. `PgCache` provides synchronous
operations, while `AsyncPgCache` offers asynchronous operations. They allow you to cache data in a PostgreSQL database
and provide functionalities to set, get, delete, and import/export cache entries.

## Installation

Before using these classes, ensure you have the following Python packages installed:

```bash
pip install -r requirements.txt
```

## Usage

### PgCache

#### Initialization

```python
from your_module import PgCache

db_url = "postgresql://user:password@localhost/dbname"
table_name = "cache_table"
cache = PgCache(db_url, table_name)
cache.init_db()
```

#### Set Cache

```python
cache.set("my_key", "my_value", expire_after_seconds=3600)
```

#### Set Bulk Cache

```python


entries

= [
    {"key": "key1", "value": "value1"},
    {"key": "key2", "value": "value2"}
]
cache.set_bulk(entries, expire_after_seconds=3600)
```

#### Get Cache

```python
value = cache.get("my_key")
```

#### Delete Cache

```python
cache.delete("my_key")
```

#### Delete Bulk Cache

```python
keys = ["key1", "key2"]
cache.delete_bulk(keys)
```

#### Flush Cache

```python
cache.flushdb()
```

#### Export Cache to File

```python
cache.export_to_file("cache_backup.json")
```

#### Import Cache from File

```python
cache.import_from_file("cache_backup.json")
```

### AsyncPgCache

#### Initialization

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

#### Set Cache

```python
async def set_cache():
    await cache.set("my_key", "my_value", expire_after_seconds=3600)


asyncio.run(set_cache())
```

#### Set Bulk Cache

```python
entries = [
    {"key": "key1", "value": "value1"},
    {"key": "key2", "value": "value2"}
]


async def set_bulk_cache():
    await cache.set_bulk(entries, expire_after_seconds=3600)


asyncio.run(set_bulk_cache())
```

#### Get Cache

```python
async def get_cache():
    value = await cache.get("my_key")
    print(value)


asyncio.run(get_cache())
```

#### Delete Cache

```python
async def delete_cache():
    await cache.delete("my_key")


asyncio.run(delete_cache())
```

#### Delete Bulk Cache

```python
keys = ["key1", "key2"]


async def delete_bulk_cache():
    await cache.delete_bulk(keys)


asyncio.run(delete_bulk_cache())
```

#### Flush Cache

```python
async def flush_cache():
    await cache.flushdb()


asyncio.run(flush_cache())
```

#### Export Cache to File

```python
async def export_cache():
    await cache.export_to_file("cache_backup.json")


asyncio.run(export_cache())
```

#### Import Cache from File

```python
async def import_cache():
    await cache.import_from_file("cache_backup.json")


asyncio.run(import_cache())
```

## Logging

You can control the logging level by setting the `log_level` parameter when initializing `PgCache` or `AsyncPgCache`.
For example:

```python
cache = PgCache(db_url, table_name, log_level=logging.INFO)
```

## Conclusion

`PgCache` and `AsyncPgCache` provide powerful cache management functionalities suitable for applications that need to
cache data in a PostgreSQL database. With both synchronous and asynchronous implementations, you can choose the
appropriate method based on your needs.