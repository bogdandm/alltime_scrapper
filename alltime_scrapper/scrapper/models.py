import asyncio
from itertools import chain
from sqlite3 import IntegrityError
from typing import Any, ClassVar, Dict, Iterable

import aiosqlite
import attr

from const import DB_PATH


@attr.s(auto_attribs=True)
class BaseSqliteModel:
    __table_name__: ClassVar[str] = None
    __db_lock: ClassVar[asyncio.Lock] = asyncio.Lock()

    @property
    def to_dict(self) -> Dict[str, Any]:
        return {
            attrib.name: getattr(self, attrib.name)
            for attrib in self.__attrs_attrs__
        }

    async def save(self):
        d = self.to_dict
        table = self.__table_name__
        sql = f"""
            INSERT INTO {table}({", ".join(d.keys())})
            VALUES ({', '.join('?' for _ in d.keys())})
        """
        await self.execute(sql, *d.values())

    @classmethod
    async def bulk_save(cls, *objects: 'BaseSqliteModel'):
        dicts = [o.to_dict for o in objects]
        values_pattern = f"({', '.join('?' for _ in dicts[0].keys())})"
        sql = f"""
            INSERT INTO {cls.__table_name__}({", ".join(dicts[0].keys())})
            VALUES {', '.join([values_pattern] * len(objects))}
        """
        await cls.execute(sql, *chain.from_iterable(map(dict.values, dicts)))

    @classmethod
    async def post_process(cls, unique_fields: Iterable[str] = ()):
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
        async with cls.__db_lock:
            try:
                async with aiosqlite.connect(str(DB_PATH)) as db:
                    await db.execute(sql, params)
                    await db.commit()
            except IntegrityError:
                pass


@attr.s(auto_attribs=True)
class CatalogWatch(BaseSqliteModel):
    __table_name__: ClassVar[str] = 'alltime_catalogwatch'

    name: str
    href: str
    image_href: str
    price: int
    text: str
    price_old: int = None

    @classmethod
    async def post_process(cls, unique_fields: Iterable[str] = ('name', 'href')):
        await super().post_process(unique_fields)
