import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Optional, List, Dict

from sqlalchemy import Column, String, DateTime, Integer, Text, MetaData, Index, ForeignKey, select, delete
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()


class Partition(Base):
    __tablename__ = 'partitions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)


class CacheEntry(Base):
    __tablename__ = 'cache_entries'
    key = Column(String, primary_key=True)
    value = Column(Text)
    expire_time = Column(DateTime, nullable=True)
    hit_count = Column(Integer, default=0)
    partition_id = Column(Integer, ForeignKey('partitions.id'), primary_key=True)

    partition = relationship("Partition", back_populates="cache_entries")

    __table_args__ = (
        Index('idx_key_expire_time', 'key', 'expire_time'),
        Index('idx_partition_id', 'partition_id'),
    )


Partition.cache_entries = relationship("CacheEntry", order_by=CacheEntry.key, back_populates="partition")


class PgCache:
    def __init__(self, db_url: str, table_name: str, partition_name: str = 'default', log_level: int = logging.ERROR):
        self.engine = create_engine(db_url, echo=False, pool_size=10, max_overflow=20)
        self.Session = sessionmaker(self.engine, expire_on_commit=False)
        self.table_name = table_name
        self.metadata = MetaData()
        self.partition_name = partition_name

        # 设置 SQLAlchemy 日志级别
        logging.getLogger('sqlalchemy.engine').setLevel(log_level)

        # 设置当前类的日志级别
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

    def init_db(self) -> None:
        self.logger.info("Initializing database...")
        try:
            with self.engine.begin() as conn:
                self.metadata.create_all(conn, tables=[Partition.__table__, CacheEntry.__table__])
                self.logger.info(f"Initialized database and created tables")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")

    def _get_partition(self, session) -> Partition:
        partition = session.query(Partition).filter_by(name=self.partition_name).first()
        if not partition:
            partition = Partition(name=self.partition_name)
            session.add(partition)
            session.commit()
        return partition

    def set(self, key: str, value: Any, expire_after_seconds: Optional[int] = None) -> None:
        self.logger.info(f"Setting cache entry: {key}")
        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            expire_time = None if expire_after_seconds is None else datetime.utcnow() + timedelta(
                seconds=expire_after_seconds)
            with self.Session() as session:
                partition = self._get_partition(session)
                entry = CacheEntry(key=key, value=value, expire_time=expire_time, partition_id=partition.id)
                session.merge(entry)
                session.commit()
                self.logger.info(f"Set cache entry: {key} in table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to set cache entry: {key}, error: {e}")

    def set_bulk(self, entries: List[Dict[str, Any]], expire_after_seconds: Optional[int] = None) -> None:
        self.logger.info(f"Setting bulk cache entries")
        try:
            with self.Session() as session:
                partition = self._get_partition(session)
                cache_entries = []
                for entry in entries:
                    for key, value in entry.items():
                        if isinstance(value, (dict, list)):
                            value = json.dumps(value)
                        expire_time = None if expire_after_seconds is None else datetime.utcnow() + timedelta(
                            seconds=expire_after_seconds)
                        cache_entry = CacheEntry(key=key, value=value, expire_time=expire_time,
                                                 partition_id=partition.id)
                        cache_entries.append(cache_entry)
                session.bulk_save_objects(cache_entries)
                session.commit()
                self.logger.info(f"Set {len(entries)} cache entries in table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to set bulk cache entries, error: {e}")

    def get(self, key: str) -> Optional[Any]:
        self.logger.info(f"Getting cache entry: {key}")
        try:
            with self.Session() as session:
                partition = self._get_partition(session)
                result = session.execute(
                    select(CacheEntry).filter(
                        CacheEntry.key == key,
                        CacheEntry.expire_time > datetime.utcnow(),
                        CacheEntry.partition_id == partition.id
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

    def delete(self, key: str) -> None:
        self.logger.info(f"Deleting cache entry: {key}")
        try:
            with self.Session() as session:
                partition = self._get_partition(session)
                session.execute(
                    delete(CacheEntry).where(
                        CacheEntry.key == key,
                        CacheEntry.partition_id == partition.id
                    )
                )
                session.commit()
                self.logger.info(f"Deleted cache entry: {key} from table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to delete cache entry: {key}, error: {e}")

    def delete_bulk(self, keys: List[str]) -> None:
        self.logger.info(f"Deleting bulk cache entries: {keys}")
        try:
            with self.Session() as session:
                partition = self._get_partition(session)
                session.execute(
                    delete(CacheEntry).where(
                        CacheEntry.key.in_(keys),
                        CacheEntry.partition_id == partition.id
                    )
                )
                session.commit()
                self.logger.info(f"Deleted bulk cache entries from table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to delete bulk cache entries, error: {e}")

    def flushdb(self) -> None:
        self.logger.info(f"Flushing all cache entries")
        try:
            with self.Session() as session:
                partition = self._get_partition(session)
                session.execute(
                    delete(CacheEntry).where(CacheEntry.partition_id == partition.id)
                )
                session.commit()
                self.logger.info(f"Flushed all cache entries from table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to flush cache entries, error: {e}")

    def export_to_file(self, file_path: str) -> None:
        self.logger.info(f"Exporting cache entries to file: {file_path}")
        if not file_path.endswith('.json'):
            self.logger.error(f"Invalid file type for export: {file_path}")
            return
        try:
            with self.Session() as session:
                partition = self._get_partition(session)
                result = session.execute(
                    select(CacheEntry).where(CacheEntry.partition_id == partition.id))
                entries = result.scalars().all()
                data = [
                    {
                        "key": entry.key,
                        "value": entry.value,
                        "expire_time": entry.expire_time.isoformat(),
                        "hit_count": entry.hit_count,
                        "partition_id": entry.partition_id
                    }
                    for entry in entries
                ]
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                self.logger.info(f"Exported cache entries to {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to export cache entries to file: {file_path}, error: {e}")

    def import_from_file(self, file_path: str) -> None:
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
                partition = self._get_partition(session)
                cache_entries = []
                for entry in data:
                    cache_entry = CacheEntry(
                        key=entry['key'],
                        value=entry['value'],
                        expire_time=datetime.fromisoformat(entry['expire_time']),
                        hit_count=entry['hit_count'],
                        partition_id=partition.id
                    )
                    cache_entries.append(cache_entry)
                session.bulk_save_objects(cache_entries)
                session.commit()
                self.logger.info(f"Imported cache entries from {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to import cache entries from file: {file_path}, error: {e}")


class AsyncPgCache:
    def __init__(self, db_url: str, table_name: str, partition_name: str = 'default', log_level: int = logging.ERROR):
        self.engine = create_async_engine(db_url, echo=False, pool_size=10, max_overflow=20)
        self.Session = sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
        self.table_name = table_name
        self.metadata = MetaData()
        self.partition_name = partition_name

        logging.getLogger('sqlalchemy.engine').setLevel(log_level)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

    async def init_db(self) -> None:
        self.logger.info("Initializing database...")
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(self.metadata.create_all, tables=[Partition.__table__, CacheEntry.__table__])
                self.logger.info(f"Initialized database and created tables")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")

    async def _get_partition(self, session) -> Partition:
        result = await session.execute(select(Partition).filter_by(name=self.partition_name))
        partition = result.scalar_one_or_none()
        if not partition:
            partition = Partition(name=self.partition_name)
            session.add(partition)
            await session.commit()
        return partition

    async def set(self, key: str, value: Any, expire_after_seconds: Optional[int] = None) -> None:
        self.logger.info(f"Setting cache entry: {key}")
        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            expire_time = None if expire_after_seconds is None else datetime.utcnow() + timedelta(
                seconds=expire_after_seconds)
            async with self.Session() as session:
                async with session.begin():
                    partition = await self._get_partition(session)
                    entry = CacheEntry(key=key, value=value, expire_time=expire_time, partition_id=partition.id)
                    await session.merge(entry)
                await session.commit()
                self.logger.info(f"Set cache entry: {key} in table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to set cache entry: {key}, error: {e}")

    async def set_bulk(self, entries: List[Dict[str, Any]], expire_after_seconds: Optional[int] = None) -> None:
        self.logger.info(f"Setting bulk cache entries")
        try:
            async with self.Session() as session:
                async with session.begin():
                    partition = await self._get_partition(session)
                    for entry in entries:
                        for key, value in entry.items():
                            if isinstance(value, (dict, list)):
                                value = json.dumps(value)
                            expire_time = None if expire_after_seconds is None else datetime.utcnow() + timedelta(
                                seconds=expire_after_seconds)
                            cache_entry = CacheEntry(key=key, value=value, expire_time=expire_time,
                                                     partition_id=partition.id)
                            await session.merge(cache_entry)
                await session.commit()
                self.logger.info(f"Set {len(entries)} cache entries in table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to set bulk cache entries, error: {e}")

    async def get(self, key: str) -> Optional[Any]:
        self.logger.info(f"Getting cache entry: {key}")
        try:
            async with self.Session() as session:
                async with session.begin():
                    partition = await self._get_partition(session)
                    result = await session.execute(
                        select(CacheEntry).filter_by(key=key, partition_id=partition.id)
                    )
                    entry = result.scalar_one_or_none()
                    if entry:
                        entry.hit_count += 1
                        await session.commit()
                        self.logger.info(f"Cache hit for key: {key} in table: {self.table_name}")
                        try:
                            return json.loads(entry.value)
                        except json.JSONDecodeError:
                            return entry.value
                self.logger.info(f"Cache miss for key: {key} in table: {self.table_name}")
                return None
        except Exception as e:
            self.logger.error(f"Failed to get cache entry: {key}, error: {e}")
            return None

    async def delete(self, key: str) -> None:
        self.logger.info(f"Deleting cache entry: {key}")
        try:
            async with self.Session() as session:
                async with session.begin():
                    partition = await self._get_partition(session)
                    await session.execute(
                        delete(CacheEntry).where(CacheEntry.key == key, CacheEntry.partition_id == partition.id)
                    )
                await session.commit()
                self.logger.info(f"Deleted cache entry: {key} from table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to delete cache entry: {key}, error: {e}")

    async def delete_bulk(self, keys: List[str]) -> None:
        self.logger.info(f"Deleting bulk cache entries: {keys}")
        try:
            async with self.Session() as session:
                async with session.begin():
                    partition = await self._get_partition(session)
                    await session.execute(
                        delete(CacheEntry).where(CacheEntry.key.in_(keys), CacheEntry.partition_id == partition.id)
                    )
                await session.commit()
                self.logger.info(f"Deleted bulk cache entries from table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to delete bulk cache entries, error: {e}")

    async def flushdb(self) -> None:
        self.logger.info(f"Flushing all cache entries")
        try:
            async with self.Session() as session:
                async with session.begin():
                    partition = await self._get_partition(session)
                    await session.execute(
                        delete(CacheEntry).where(CacheEntry.partition_id == partition.id)
                    )
                await session.commit()
                self.logger.info(f"Flushed all cache entries from table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to flush cache entries, error: {e}")

    async def export_to_file(self, file_path: str) -> None:
        self.logger.info(f"Exporting cache entries to file: {file_path}")
        if not file_path.endswith('.json'):
            self.logger.error(f"Invalid file type for export: {file_path}")
            return
        try:
            async with self.Session() as session:
                async with session.begin():
                    partition = await self._get_partition(session)
                    result = await session.execute(
                        select(CacheEntry).where(CacheEntry.partition_id == partition.id))
                    entries = result.scalars().all()
                    data = [
                        {
                            'key': entry.key,
                            'value': entry.value,
                            'expire_time': entry.expire_time.isoformat() if entry.expire_time else None,
                            'hit_count': entry.hit_count,
                            'partition_id': entry.partition_id
                        }
                        for entry in entries
                    ]
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=4)
                self.logger.info(f"Exported cache entries to {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to export cache entries to file: {file_path}, error: {e}")

    async def import_from_file(self, file_path: str) -> None:
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
                    partition = await self._get_partition(session)
                    cache_entries = []
                    for entry in data:
                        cache_entry = CacheEntry(
                            key=entry['key'],
                            value=entry['value'],
                            expire_time=datetime.fromisoformat(entry['expire_time']) if entry['expire_time'] else None,
                            hit_count=entry['hit_count'],
                            partition_id=partition.id
                        )
                        cache_entries.append(cache_entry)
                    session.add_all(cache_entries)
                await session.commit()
                self.logger.info(f"Imported cache entries from {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to import cache entries from file: {file_path}, error: {e}")
