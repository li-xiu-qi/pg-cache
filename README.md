# PgCache

`PgCache` is a synchronous cache library based on PostgreSQL, using SQLAlchemy for database operations (the asynchronous
version is called AsyncPgCache).

## Installation

First, ensure you have installed the required dependencies:

```bash
pip install sqlalchemy psycopg2
```

## Usage

### Initialize Cache

```python
import logging
from pg_cache.sync_cache import PgCache

DATABASE_URL = "postgresql://username:password@localhost:5432/dbname"


def main():
    cache = PgCache(DATABASE_URL, "cache_table", log_level=logging.ERROR)

    # Initialize the database
    cache.init_db()

    # Set cache
    cache.set("test_key", {"foo": "bar"}, expire_after_seconds=60)
    print("Set cache entry")

    # Get cache
    value = cache.get("test_key")
    print(f"Got cache entry: {value}")

    # Delete cache
    cache.delete("test_key")
    print("Deleted cache entry")

    # Get deleted cache
    value = cache.get("test_key")
    print(f"Got cache entry after deletion: {value}")


if __name__ == "__main__":
    main()
```

### Method Descriptions

#### `__init__(self, db_url: str, table_name: str, log_level: int = logging.ERROR)`

Initialize a `PgCache` instance.

- `db_url`: Database connection string.
- `table_name`: Name of the cache table.
- `log_level`: Log level, default is `logging.ERROR`.

#### `init_db(self) -> None`

Initialize the database and create the cache table.

#### `set(self, key: str, value: Any, expire_after_seconds: int = 86400, partition_key: str = 'default') -> None`

Set a cache entry.

- `key`: Cache key.
- `value`: Cache value, can be any type of data.
- `expire_after_seconds`: Cache expiration time (seconds), default is 86400 seconds (1 day).
- `partition_key`: Partition key, default is `default`.

####
`set_bulk(self, entries: List[Dict[str, Any]], expire_after_seconds: int = 86400, partition_key: str = 'default') -> None`

Set multiple cache entries in bulk.

- `entries`: List of multiple cache entries, each entry is a dictionary containing `key` and `value`.
- `expire_after_seconds`: Cache expiration time (seconds), default is 86400 seconds (1 day).
- `partition_key`: Partition key, default is `default`.

#### `get(self, key: str, partition_key: str = 'default') -> Optional[Any]`

Get a cache entry.

- `key`: Cache key.
- `partition_key`: Partition key, default is `default`.
- Return value: Cache value, if the cache entry does not exist or has expired, returns `None`.

#### `delete(self, key: str, partition_key: str = 'default') -> None`

Delete a cache entry.

- `key`: Cache key.
- `partition_key`: Partition key, default is `default`.

#### `flushdb(self, partition_key: str = 'default') -> None`

Clear all cache entries.

- `partition_key`: Partition key, default is `default`.

#### `export_to_file(self, file_path: str, partition_key: str = 'default') -> None`

Export cache entries to a file.

- `file_path`: Path to the export file.
- `partition_key`: Partition key, default is `default`.

#### `import_from_file(self, file_path: str, partition_key: str = 'default') -> None`

Import cache entries from a file.

- `file_path`: Path to the import file.
- `partition_key`: Partition key, default is `default`.

## Example

Here is a complete example demonstrating how to use the `PgCache` class:

```python
import logging
from pg_cache.sync_cache import PgCache

DATABASE_URL = "postgresql://username:password@localhost:5432/dbname"


def main():
    cache = PgCache(DATABASE_URL, "cache_table", log_level=logging.ERROR)

    # Initialize the database
    cache.init_db()

    # Set cache
    cache.set("test_key", {"foo": "bar"}, expire_after_seconds=60)
    print("Set cache entry")

    # Get cache
    value = cache.get("test_key")
    print(f"Got cache entry: {value}")

    # Delete cache
    cache.delete("test_key")
    print("Deleted cache entry")

    # Get deleted cache
    value = cache.get("test_key")
    print(f"Got cache entry after deletion: {value}")


if __name__ == "__main__":
    main()
```

## License

This project is licensed under the MIT License.