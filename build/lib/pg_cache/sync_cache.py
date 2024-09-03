import logging
import json
import os
from sqlalchemy import create_engine, Column, String, DateTime, Integer, Text, MetaData, Index, select, delete
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime, timedelta
from typing import Any, Optional, List, Dict

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

class PgCache:
    def __init__(self, db_url: str, table_name: str, log_level: int = logging.ERROR):
        self.engine = create_engine(db_url, echo=False, pool_size=20, max_overflow=30)
        self.Session = sessionmaker(bind=self.engine)
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

    def set(self, key: str, value: Any, expire_after_seconds: int = 86400) -> None:
        self.logger.info(f"Setting cache entry: {key}")
        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            expire_time = datetime.utcnow() + timedelta(seconds=expire_after_seconds)
            entry = self.CacheEntry(key=key, value=value, expire_time=expire_time)
            with self.Session() as session:
                session.merge(entry)
                session.commit()
                self.logger.info(f"Set cache entry: {key} in table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to set cache entry: {key}, error: {e}")

    def set_bulk(self, entries: List[Dict[str, Any]], expire_after_seconds: int = 86400) -> None:
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
                    cache_entry = self.CacheEntry(key=key, value=value, expire_time=expire_time)
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
                result = session.execute(
                    select(self.CacheEntry).filter(
                        self.CacheEntry.key == key,
                        self.CacheEntry.expire_time > datetime.utcnow()
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
                session.execute(
                    delete(self.CacheEntry).where(self.CacheEntry.key == key)
                )
                session.commit()
                self.logger.info(f"Deleted cache entry: {key} from table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to delete cache entry: {key}, error: {e}")

    def delete_bulk(self, keys: List[str]) -> None:
        self.logger.info(f"Deleting bulk cache entries: {keys}")
        try:
            with self.Session() as session:
                session.execute(
                    delete(self.CacheEntry).where(self.CacheEntry.key.in_(keys))
                )
                session.commit()
                self.logger.info(f"Deleted bulk cache entries from table: {self.table_name}")
        except Exception as e:
            self.logger.error(f"Failed to delete bulk cache entries, error: {e}")

    def flushdb(self) -> None:
        self.logger.info(f"Flushing all cache entries")
        try:
            with self.Session() as session:
                session.execute(
                    delete(self.CacheEntry)
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
                result = session.execute(select(self.CacheEntry))
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
                session.commit()
                self.logger.info(f"Imported cache entries from {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to import cache entries from file: {file_path}, error: {e}")