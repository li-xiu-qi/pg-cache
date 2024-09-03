# PgCache

`PgCache` 是一个基于 PostgreSQL 的同步缓存库，使用 SQLAlchemy 进行数据库操作（异步版本叫做AsyncPgCache）。

## 安装

首先，确保你已经安装了所需的依赖项：

```bash
pip install sqlalchemy psycopg2
```

## 使用方法

### 初始化缓存

```python
import logging
from pg_cache.sync_cache import PgCache

DATABASE_URL = "postgresql://username:password@localhost:5432/dbname"


def main():
    cache = PgCache(DATABASE_URL, "cache_table", log_level=logging.ERROR)

    # 初始化数据库
    cache.init_db()

    # 设置缓存
    cache.set("test_key", {"foo": "bar"}, expire_after_seconds=60)
    print("Set cache entry")

    # 获取缓存
    value = cache.get("test_key")
    print(f"Got cache entry: {value}")

    # 删除缓存
    cache.delete("test_key")
    print("Deleted cache entry")

    # 获取已删除的缓存
    value = cache.get("test_key")
    print(f"Got cache entry after deletion: {value}")


if __name__ == "__main__":
    main()
```

### 方法说明

#### `__init__(self, db_url: str, table_name: str, log_level: int = logging.ERROR)`

初始化 `PgCache` 实例。

- `db_url`: 数据库连接字符串。
- `table_name`: 缓存表的名称。
- `log_level`: 日志级别，默认为 `logging.ERROR`。

#### `init_db(self) -> None`

初始化数据库并创建缓存表。

#### `set(self, key: str, value: Any, expire_after_seconds: int = 86400, partition_key: str = 'default') -> None`

设置缓存条目。

- `key`: 缓存键。
- `value`: 缓存值，可以是任意类型的数据。
- `expire_after_seconds`: 缓存过期时间（秒），默认为 86400 秒（1 天）。
- `partition_key`: 分区键，默认为 `default`。

####
`set_bulk(self, entries: List[Dict[str, Any]], expire_after_seconds: int = 86400, partition_key: str = 'default') -> None`

批量设置缓存条目。

- `entries`: 包含多个缓存条目的列表，每个条目是一个字典，包含 `key` 和 `value`。
- `expire_after_seconds`: 缓存过期时间（秒），默认为 86400 秒（1 天）。
- `partition_key`: 分区键，默认为 `default`。

#### `get(self, key: str, partition_key: str = 'default') -> Optional[Any]`

获取缓存条目。

- `key`: 缓存键。
- `partition_key`: 分区键，默认为 `default`。
- 返回值：缓存值，如果缓存条目不存在或已过期，则返回 `None`。

#### `delete(self, key: str, partition_key: str = 'default') -> None`

删除缓存条目。

- `key`: 缓存键。
- `partition_key`: 分区键，默认为 `default`。

#### `flushdb(self, partition_key: str = 'default') -> None`

清空所有缓存条目。

- `partition_key`: 分区键，默认为 `default`。

#### `export_to_file(self, file_path: str, partition_key: str = 'default') -> None`

将缓存条目导出到文件。

- `file_path`: 导出文件的路径。
- `partition_key`: 分区键，默认为 `default`。

#### `import_from_file(self, file_path: str, partition_key: str = 'default') -> None`

从文件导入缓存条目。

- `file_path`: 导入文件的路径。
- `partition_key`: 分区键，默认为 `default`。

## 示例

以下是一个完整的示例，展示了如何使用 `PgCache` 类：

```python
import logging
from pg_cache.sync_cache import PgCache

DATABASE_URL = "postgresql://username:password@localhost:5432/dbname"


def main():
    cache = PgCache(DATABASE_URL, "cache_table", log_level=logging.ERROR)

    # 初始化数据库
    cache.init_db()

    # 设置缓存
    cache.set("test_key", {"foo": "bar"}, expire_after_seconds=60)
    print("Set cache entry")

    # 获取缓存
    value = cache.get("test_key")
    print(f"Got cache entry: {value}")

    # 删除缓存
    cache.delete("test_key")
    print("Deleted cache entry")

    # 获取已删除的缓存
    value = cache.get("test_key")
    print(f"Got cache entry after deletion: {value}")


if __name__ == "__main__":
    main()
```

## 许可证

此项目使用 MIT 许可证。