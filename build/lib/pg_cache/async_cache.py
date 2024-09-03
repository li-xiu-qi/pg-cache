import json
import logging
from datetime import datetime, timedelta
from typing import Any, Optional, List, Dict

from sqlalchemy import Column, String, DateTime, Integer, Text, MetaData, Index, select, delete
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class CacheEntry(Base):
    __abstract__ = True

    key = Column(String, primary_key=True)
    value = Column(Text)
    expire_time = Column(DateTime)
    hit_count = Column(Integer, default=0)

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    __table_args__ = (
        Index('idx_key_expire_time', 'key', 'expire_time'),
    )


class AsyncPgCache:
    def __init__(self, db_url: str, table_name: str, log_level: int = logging.ERROR):
        self.engine = create_async_engine(db_url, echo=False, pool_size=10, max_overflow=20)
        self.Session = sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
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

    async def init_db(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(self.metadata.create_all, tables=[self.CacheEntry.__table__])
            self.logger.info(f"Initialized database and created table: {self.table_name}")

    async def set(self, key: str, value: Any, expire_after_seconds: int = 86400) -> None:
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        expire_time = datetime.utcnow() + timedelta(seconds=expire_after_seconds)
        entry = self.CacheEntry(key=key, value=value, expire_time=expire_time)
        async with self.Session() as session:
            async with session.begin():
                await session.merge(entry)
            await session.commit()
            self.logger.info(f"Set cache entry: {key} in table: {self.table_name}")

    async def set_bulk(self, entries: List[Dict[str, Any]], expire_after_seconds: int = 86400) -> None:
        async with self.Session() as session:
            async with session.begin():
                cache_entries = []
                for entry in entries:
                    key = entry['key']
                    value = entry['value']
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value)
                    expire_time = datetime.utcnow() + timedelta(seconds=expire_after_seconds)
                    cache_entry = self.CacheEntry(key=key, value=value, expire_time=expire_time)
                    cache_entries.append(cache_entry)
                session.bulk_save_objects(cache_entries)
            await session.commit()
            self.logger.info(f"Set {len(entries)} cache entries in table: {self.table_name}")

    async def get(self, key: str) -> Optional[Any]:
        async with self.Session() as session:
            result = await session.execute(
                select(self.CacheEntry).filter(
                    self.CacheEntry.key == key,
                    self.CacheEntry.expire_time > datetime.utcnow()
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

    async def delete(self, key: str) -> None:
        async with self.Session() as session:
            await session.execute(
                delete(self.CacheEntry).where(self.CacheEntry.key == key)
            )
            await session.commit()
            self.logger.info(f"Deleted cache entry: {key} from table: {self.table_name}")

    async def flushdb(self) -> None:
        async with self.Session() as session:
            await session.execute(
                delete(self.CacheEntry)
            )
            await session.commit()
            self.logger.info(f"Flushed all cache entries from table: {self.table_name}")

    async def export_to_file(self, file_path: str) -> None:
        async with self.Session() as session:
            result = await session.execute(select(self.CacheEntry))
            entries = result.scalars().all()
            data = [
                {
                    "key": entry.key,
                    "value": entry.value,
                    "expire_time": entry.expire_time.isoformat(),
                    "hit_count": entry.hit_count
                }
                for entry in entries
            ]
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            self.logger.info(f"Exported cache entries to {file_path}")

    async def import_from_file(self, file_path: str) -> None:
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
                        hit_count=entry['hit_count']
                    )
                    cache_entries.append(cache_entry)
                session.bulk_save_objects(cache_entries)
            await session.commit()
            self.logger.info(f"Imported cache entries from {file_path}")