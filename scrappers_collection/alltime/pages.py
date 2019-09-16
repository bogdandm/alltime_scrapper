from typing import Any, AsyncGenerator, ClassVar, Dict, Iterable, List, Optional, Tuple

import attr
from requests_html import HTML, Element

from .catalog import CatalogWatch
from .const import DOMAIN, HTML_ENCODING
from ..core.downloader import BaseDownloader
from ..core.models import BaseSqliteModel
from ..core.scrapper import BaseScrapper


class MissingPageDownloader(BaseDownloader):
    BASE_URL = DOMAIN
    QUEUE_MAXSIZE = 50

    def __init__(self, encoding: str, connections: int, retry_after: float):
        super().__init__(encoding, connections, retry_after, queue_maxsize=self.QUEUE_MAXSIZE)

    @property
    async def urls(self) -> AsyncGenerator[str, Any]:
        async for item_id, href in Watch.iter_missing():
            self.context[DOMAIN + href] = {'catalog_id': item_id}
        for url in self.context.keys():
            yield url


@attr.s(auto_attribs=True)
class Watch(BaseSqliteModel['Watch']):
    __table_name__: ClassVar[str] = 'alltime_watch'

    catalog_page_id: int
    core_type: Optional[str] = None
    case: Optional[str] = None
    face: Optional[str] = None
    bracelet: Optional[str] = None
    water: Optional[str] = None
    light: Optional[str] = None
    glass: Optional[str] = None
    calendar: Optional[str] = None
    signal: Optional[str] = None
    size_raw: Optional[str] = None
    country: Optional[str] = None
    text: str = ''
    id: Optional[int] = None
    __catalog_page: Optional[CatalogWatch] = None

    @property
    async def catalog_page(self) -> Optional[CatalogWatch]:
        if not self.__catalog_page:
            async for page in CatalogWatch.load(dict(id=self.catalog_page_id)):
                self.__catalog_page = page
        return self.__catalog_page

    @catalog_page.setter
    async def catalog_page(self, page: CatalogWatch):
        self.__catalog_page = page
        self.catalog_page_id = page.id
        await self.save('catalog_page_id')

    @classmethod
    async def drop_duplicates(cls, unique_fields: Iterable[str] = ('catalog_page',)):
        await super().drop_duplicates(unique_fields)

    @classmethod
    async def iter_missing(cls) -> AsyncGenerator[Tuple[int, str], Any]:
        # noinspection SqlResolve
        sql = f"""
            SELECT C.id, C.href
            FROM {CatalogWatch.table()} C 
            LEFT OUTER JOIN {cls.table()} W ON C.id = W.catalog_page_id
            WHERE W.id IS NULL
            ORDER BY 2
            -- LIMIT 100;
        """
        async for item_id, href in cls.raw_select(sql):
            yield item_id, href


class WatchPageScrapper(BaseScrapper):
    _mapping = {
        'Артикул/модель': ...,
        'Калибр': ...,
        'Технические особенности': ...,

        'Камень-вставка': ...,
        'Общий вес золота (г)': ...,

        'Тип механизма': 'core_type',
        'Корпус': 'case',
        'Циферблат': 'face',
        'Браслет': 'bracelet',
        'Водозащита': 'water',
        'Подсветка': 'light',
        'Стекло': 'glass',
        'Календарь': 'calendar',
        'Звуковой сигнал': 'signal',
        'Габаритные размеры': 'size_raw',
        'Страна': 'country',
    }

    @classmethod
    def process_html(cls, html: str, context: Optional[Dict[str, Any]] = None) -> List[Watch]:
        context: Dict[str, Any] = context or {}
        html_doc: HTML = cls.parse_html(html, encoding=HTML_ENCODING)
        node: Element = html_doc.find('.col-content', first=True)
        fields: Dict[str, str] = {}
        for row in node.find('.product-accordion div[data-id="2"] table > tr'):
            th, td = row.find('th', first=True), row.find('td', first=True)
            td.pq('td').find('.js-hintme-this').remove()
            key = th.text.strip()
            value = td.text.strip()
            if key not in cls._mapping:
                print(key, '->', value)
            else:
                field = cls._mapping[key]
                if field is Ellipsis:
                    continue
                fields[cls._mapping[key]] = value
        return [Watch(
            catalog_page_id=context.get('catalog_id', None),
            **fields
        )]
