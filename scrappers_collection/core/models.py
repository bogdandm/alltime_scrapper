import asyncio
from itertools import chain
from sqlite3.dbapi2 import IntegrityError
from typing import Any, AsyncGenerator, ClassVar, Dict, Iterable, TypeVar, Generic, Optional, Union

import aiosqlite
import attr

from ..const import DB_PATH

InstanceType = TypeVar('InstanceType', bound='BaseSqliteModel')


@attr.s(auto_attribs=True)
class BaseSqliteModel(Generic[InstanceType]):
    __table_name__: ClassVar[Optional[str]] = None
    __db_lock: ClassVar[Optional[asyncio.Lock]] = None

    @classmethod
    def table(cls):
        return cls.__table_name__

    @classmethod
    def _get_lock(cls):
        if not cls.__db_lock:
            cls.__db_lock = asyncio.Lock()
        return cls.__db_lock

    @property
    def to_dict(self) -> Dict[str, Any]:
        return {
            attrib.name: getattr(self, attrib.name)
            for attrib in self.__attrs_attrs__
            if not attrib.name.startswith('_')
        }

    @property
    @classmethod
    def columns(cls):
        return [
            attrib.name
            for attrib in cls.__attrs_attrs__
            if not attrib.name.startswith('_')
        ]

    async def save(self, *fields):
        d = {
            column: v
            for column, v in self.to_dict.items()
            if not fields or column in fields
        }
        table = self.__table_name__
        sql = f"""
            INSERT INTO {table}({", ".join(f"'{f}'" for f in d.keys())})
            VALUES ({', '.join('?' for _ in d.keys())})
        """
        await self.execute(sql, *d.values())

    @classmethod
    async def bulk_save(cls, *objects: InstanceType):
        dicts = [o.to_dict for o in objects]
        fields = list(dicts[0].keys())
        values_pattern = f"({', '.join('?' for _ in fields)})"
        sql = f"""
            INSERT INTO {cls.__table_name__}({", ".join(f"'{f}'" for f in fields)})
            VALUES {', '.join([values_pattern] * len(objects))}
        """
        await cls.execute(sql, *chain.from_iterable(map(dict.values, dicts)))

    @classmethod
    async def load(cls, filters: Optional[Dict[str, Any]] = None) -> AsyncGenerator[InstanceType, Any]:
        filters = filters or {}
        where = ' AND '.join(filters.keys())
        sql = f"SELECT {', '.join(cls.columns)} FROM {cls.__table_name__}"
        if where:
            sql += f" WHERE {where}"

        o: InstanceType
        async for o in cls.select(sql, list(filters.values())):
            yield o

    @classmethod
    async def drop_duplicates(cls, unique_fields: Iterable[str] = ()):
        sql = f"""
            DELETE FROM {cls.__table_name__}
            WHERE id NOT IN (
               SELECT MIN(id) as id
               FROM {cls.__table_name__} 
               GROUP BY {', '.join(unique_fields)}
            )
        """
        await cls.execute(sql)

    @classmethod
    async def execute(cls, sql, *params):
        async with cls._get_lock():
            try:
                async with aiosqlite.connect(str(DB_PATH)) as db:
                    await db.execute(sql, params)
                    await db.commit()
            except IntegrityError:
                pass

    @classmethod
    async def select(cls, sql, *params) -> AsyncGenerator[InstanceType, Any]:
        async for row in cls.raw_select(sql, *params):
            yield cls(*row)

    @classmethod
    async def raw_select(cls, sql, *params) -> AsyncGenerator[list, Any]:
        async with cls._get_lock():
            try:
                async with aiosqlite.connect(str(DB_PATH)) as db:
                    async  with db.execute(sql, params) as cursor:
                        async for row in cursor:
                            yield row
            except IntegrityError:
                pass
