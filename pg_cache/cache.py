import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Optional, List, Dict
import json
import os

from sqlalchemy import Column, String, DateTime, Integer, Text, MetaData, Index, select, delete, PrimaryKeyConstraint
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class CacheEntry(Base):
    __abstract__ = True

    key = Column(String)
    value = Column(Text)
    expire_time = Column(DateTime)
    hit_count = Column(Integer, default=0)
    partition_key = Column(String, default='default_partition')

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    __table_args__ = (
        PrimaryKeyConstraint('key', 'partition_key', name='pk_key_partition_key'),
        Index('idx_key_expire_time', 'key', 'expire_time'),
        Index('idx_partition_key', 'partition_key'),
    )

class PgCache:
    def __init__(self, db_url: str, table_name: str, log_level: int = logging.ERROR):
        self.engine = create_engine(db_url, echo=False, pool_size=10, max_overflow=20)
        self.Session = sessionmaker(self.engine, expire_on_commit=False)
        self.table_name = table_name
        self.metadata = MetaData()

        # 动态创建表
        self.CacheEntry = type(
            table_name,
            (CacheEntry, Base),
            {'__tablename__': table_name, '__table_args__': {'extend_existing': True}}
        )

        # 设置 SQLAlchemy 日志级别
        logging.getLogger('sqlalchemy.engine').setLevel(log_level)

        # 设置当前类的日志级别
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

    def init_db(self) -> None:
        self.logger.info("Initializing database...")
        try:
            with self.engine.begin() as conn:
                self.metadata.create_all(conn, tables=[self.CacheEntry.__table__])
                self.logger.info(f"Initialized database and created table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")

    def set(self, key: str, value: Any, expire_after_seconds: int = 86400,
            partition_key: str = 'default_partition') -> None:
        self.logger.info(f"Setting cache entry: {key}")
        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            expire_time = datetime.utcnow() + timedelta(seconds=expire_after_seconds)
            entry = self.CacheEntry(key=key, value=value, expire_time=expire_time, partition_key=partition_key)
            with self.Session() as session:
                session.merge(entry)
                session.commit()
                self.logger.info(f"Set cache entry: {key} in table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to set cache entry: {key}, error: {e}")

    def set_bulk(self, entries: List[Dict[str, Any]], expire_after_seconds: int = 86400,
                 partition_key: str = 'default_partition') -> None:
        self.logger.info(f"Setting bulk cache entries")
        try:
            with self.Session() as session:
                cache_entries = []
                for entry in entries:
                    key = entry['key']
                    value = entry['value']
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value)
                    expire_time = datetime.utcnow() + timedelta(seconds=expire_after_seconds)
                    cache_entry = self.CacheEntry(key=key, value=value, expire_time=expire_time,
                                                  partition_key=partition_key)
                    cache_entries.append(cache_entry)
                session.bulk_save_objects(cache_entries)
                session.commit()
                self.logger.info(f"Set {len(entries)} cache entries in table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to set bulk cache entries, error: {e}")

    def get(self, key: str, partition_key: str = 'default_partition') -> Optional[Any]:
        self.logger.info(f"Getting cache entry: {key}")
        try:
            with self.Session() as session:
                result = session.execute(
                    select(self.CacheEntry).filter(
                        self.CacheEntry.key == key,
                        self.CacheEntry.expire_time > datetime.utcnow(),
                        self.CacheEntry.partition_key == partition_key
                    )
                )
                entry = result.scalar_one_or_none()
                if entry:
                    entry.hit_count += 1
                    session.commit()
                    self.logger.info(
                        f"Retrieved cache entry: {key} (hit count: {entry.hit_count}) from table: {self.table_name}")
                    try:
                        return json.loads(entry.value)
                    except json.JSONDecodeError:
                        return entry.value
                self.logger.info(f"Cache miss for key: {key} in table: {self.table_name}")
                return None
        except Exception as e:
            self.logger.error(f"Failed to get cache entry: {key}, error: {e}")
            return None

    def delete(self, key: str, partition_key: str = 'default_partition') -> None:
        self.logger.info(f"Deleting cache entry: {key}")
        try:
            with self.Session() as session:
                session.execute(
                    delete(self.CacheEntry).where(
                        self.CacheEntry.key == key,
                        self.CacheEntry.partition_key == partition_key
                    )
                )
                session.commit()
                self.logger.info(f"Deleted cache entry: {key} from table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to delete cache entry: {key}, error: {e}")

    def delete_bulk(self, keys: List[str], partition_key: str = 'default_partition') -> None:
        self.logger.info(f"Deleting bulk cache entries: {keys}")
        try:
            with self.Session() as session:
                session.execute(
                    delete(self.CacheEntry).where(
                        self.CacheEntry.key.in_(keys),
                        self.CacheEntry.partition_key == partition_key
                    )
                )
                session.commit()
                self.logger.info(f"Deleted bulk cache entries from table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to delete bulk cache entries, error: {e}")

    def flushdb(self, partition_key: str = 'default_partition') -> None:
        self.logger.info(f"Flushing all cache entries")
        try:
            with self.Session() as session:
                session.execute(
                    delete(self.CacheEntry).where(self.CacheEntry.partition_key == partition_key)
                )
                session.commit()
                self.logger.info(f"Flushed all cache entries from table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to flush cache entries, error: {e}")

    def export_to_file(self, file_path: str, partition_key: str = 'default_partition') -> None:
        self.logger.info(f"Exporting cache entries to file: {file_path}")
        if not file_path.endswith('.json'):
            self.logger.error(f"Invalid file type for export: {file_path}")
            return
        try:
            with self.Session() as session:
                result = session.execute(select(self.CacheEntry).where(self.CacheEntry.partition_key == partition_key))
                entries = result.scalars().all()
                data = [
                    {
                        "key": entry.key,
                        "value": entry.value,
                        "expire_time": entry.expire_time.isoformat(),
                        "hit_count": entry.hit_count,
                        "partition_key": entry.partition_key
                    }
                    for entry in entries
                ]
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                self.logger.info(f"Exported cache entries to {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to export cache entries to file: {file_path}, error: {e}")

    def import_from_file(self, file_path: str, partition_key: str = 'default_partition') -> None:
        self.logger.info(f"Importing cache entries from file: {file_path}")
        if not file_path.endswith('.json'):
            self.logger.error(f"Invalid file type for import: {file_path}")
            return
        if not os.path.exists(file_path):
            self.logger.error(f"File does not exist: {file_path}")
            return
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            with self.Session() as session:
                cache_entries = []
                for entry in data:
                    cache_entry = self.CacheEntry(
                        key=entry['key'],
                        value=entry['value'],
                        expire_time=datetime.fromisoformat(entry['expire_time']),
                        hit_count=entry['hit_count'],
                        partition_key=partition_key
                    )
                    cache_entries.append(cache_entry)
                session.bulk_save_objects(cache_entries)
                session.commit()
                self.logger.info(f"Imported cache entries from {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to import cache entries from file: {file_path}, error: {e}")


class AsyncPgCache(PgCache):
    def __init__(self, db_url: str, table_name: str, log_level: int = logging.ERROR):
        super().__init__(db_url, table_name, log_level)
        self.engine = create_async_engine(db_url, echo=False, pool_size=10, max_overflow=20)
        self.Session = sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)

    async def init_db(self) -> None:
        self.logger.info("Initializing database...")
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(self.metadata.create_all, tables=[self.CacheEntry.__table__])
                self.logger.info(f"Initialized database and created table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")

    async def set(self, key: str, value: Any, expire_after_seconds: int = 86400,
                  partition_key: str = 'default_partition') -> None:
        self.logger.info(f"Setting cache entry: {key}")
        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            expire_time = datetime.utcnow() + timedelta(seconds=expire_after_seconds)
            entry = self.CacheEntry(key=key, value=value, expire_time=expire_time, partition_key=partition_key)
            async with self.Session() as session:
                async with session.begin():
                    await session.merge(entry)
                await session.commit()
                self.logger.info(f"Set cache entry: {key} in table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to set cache entry: {key}, error: {e}")

    async def set_bulk(self, entries: List[Dict[str, Any]], expire_after_seconds: int = 86400,
                       partition_key: str = 'default_partition') -> None:
        self.logger.info(f"Setting bulk cache entries")
        try:
            async with self.Session() as session:
                async with session.begin():
                    for entry in entries:
                        key = entry['key']
                        value = entry['value']
                        if isinstance(value, (dict, list)):
                            value = json.dumps(value)
                        expire_time = datetime.utcnow() + timedelta(seconds=expire_after_seconds)
                        cache_entry = self.CacheEntry(key=key, value=value, expire_time=expire_time,
                                                      partition_key=partition_key)
                        session.add(cache_entry)
                await session.commit()
                self.logger.info(f"Set {len(entries)} cache entries in table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to set bulk cache entries, error: {e}")

    async def get(self, key: str, partition_key: str = 'default_partition') -> Optional[Any]:
        self.logger.info(f"Getting cache entry: {key}")
        try:
            async with self.Session() as session:
                result = await session.execute(
                    select(self.CacheEntry).filter(
                        self.CacheEntry.key == key,
                        self.CacheEntry.expire_time > datetime.utcnow(),
                        self.CacheEntry.partition_key == partition_key
                    )
                )
                entry = result.scalar_one_or_none()
                if entry:
                    entry.hit_count += 1
                    await session.commit()
                    self.logger.info(
                        f"Retrieved cache entry: {key} (hit count: {entry.hit_count}) from table: {self.table_name}")
                    try:
                        return json.loads(entry.value)
                    except json.JSONDecodeError:
                        return entry.value
                self.logger.info(f"Cache miss for key: {key} in table: {self.table_name}")
                return None
        except Exception as e:
            self.logger.error(f"Failed to get cache entry: {key}, error: {e}")
            return None

    async def delete(self, key: str, partition_key: str = 'default_partition') -> None:
        self.logger.info(f"Deleting cache entry: {key}")
        try:
            async with self.Session() as session:
                await session.execute(
                    delete(self.CacheEntry).where(
                        self.CacheEntry.key == key,
                        self.CacheEntry.partition_key == partition_key
                    )
                )
                await session.commit()
                self.logger.info(f"Deleted cache entry: {key} from table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to delete cache entry: {key}, error: {e}")

    async def delete_bulk(self, keys: List[str], partition_key: str = 'default_partition') -> None:
        self.logger.info(f"Deleting bulk cache entries: {keys}")
        try:
            async with self.Session() as session:
                await session.execute(
                    delete(self.CacheEntry).where(
                        self.CacheEntry.key.in_(keys),
                        self.CacheEntry.partition_key == partition_key
                    )
                )
                await session.commit()
                self.logger.info(f"Deleted bulk cache entries from table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to delete bulk cache entries, error: {e}")

    async def flushdb(self, partition_key: str = 'default_partition') -> None:
        self.logger.info(f"Flushing all cache entries")
        try:
            async with self.Session() as session:
                await session.execute(
                    delete(self.CacheEntry).where(self.CacheEntry.partition_key == partition_key)
                )
                await session.commit()
                self.logger.info(f"Flushed all cache entries from table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to flush cache entries, error: {e}")

    async def export_to_file(self, file_path: str, partition_key: str = 'default_partition') -> None:
        self.logger.info(f"Exporting cache entries to file: {file_path}")
        if not file_path.endswith('.json'):
            self.logger.error(f"Invalid file type for export: {file_path}")
            return
        try:
            async with self.Session() as session:
                result = await session.execute(select(self.CacheEntry).where(self.CacheEntry.partition_key == partition_key))
                entries = result.scalars().all()
                data = [
                    {
                        "key": entry.key,
                        "value": entry.value,
                        "expire_time": entry.expire_time.isoformat(),
                        "hit_count": entry.hit_count,
                        "partition_key": entry.partition_key
                    }
                    for entry in entries
                ]
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                self.logger.info(f"Exported cache entries to {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to export cache entries to file: {file_path}, error: {e}")

    async def import_from_file(self, file_path: str, partition_key: str = 'default_partition') -> None:
        self.logger.info(f"Importing cache entries from file: {file_path}")
        if not file_path.endswith('.json'):
            self.logger.error(f"Invalid file type for import: {file_path}")
            return
        if not os.path.exists(file_path):
            self.logger.error(f"File does not exist: {file_path}")
            return
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            async with self.Session() as session:
                async with session.begin():
                    cache_entries = []
                    for entry in data:
                        cache_entry = self.CacheEntry(
                            key=entry['key'],
                            value=entry['value'],
                            expire_time=datetime.fromisoformat(entry['expire_time']),
                            hit_count=entry['hit_count'],
                            partition_key=partition_key
                        )
                        cache_entries.append(cache_entry)
                    session.bulk_save_objects(cache_entries)
                await session.commit()
                self.logger.info(f"Imported cache entries from {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to import cache entries from file: {file_path}, error: {e}")

