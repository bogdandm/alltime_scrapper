from typing import ClassVar, Iterable, List

import attr
from requests_html import HTML

from .const import CATALOG_PAGE, CATALOG_PAGES, HTML_ENCODING
from ..core.downloader import BaseDownloader
from ..core.models import BaseSqliteModel
from ..core.scrapper import BaseScrapper


class CatalogDownloader(BaseDownloader):
    BASE_URL = CATALOG_PAGE

    @property
    def urls(self):
        yield self.BASE_URL
        for page in range(2, CATALOG_PAGES + 1):
            yield f"{self.BASE_URL}?PAGEN_1={page}"

    @property
    def cookies(self):
        return {
            "ALLTIME_COUNT_PAGE": "100",
            "ALLTIME_SORT_BY": "ID"
        }


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


class CatalogScrapper(BaseScrapper):
    @classmethod
    def process_html(cls, html: str) -> List[CatalogWatch]:
        html_doc: HTML = cls.parse_html(html, encoding=HTML_ENCODING)
        res: List[CatalogWatch] = []
        for card in html_doc.find('.bcc-post'):
            image = card.find('.bcc-image .first_image', first=True)
            price_new, *price_old = card.find('.bcc-price', first=True).find('li')
            price_old = price_old and price_old[0] or None
            if price_old:
                price_old = cls.process_number(price_old.text)

            res.append(CatalogWatch(
                name=card.find('.bcc-title', first=True).text.strip().split('\n')[0].strip(),
                href=image.attrs['data-href'],
                image_href=image.find('img', first=True).attrs['src'],
                price=cls.process_number(price_new.text),
                price_old=price_old,
                text=card.find('.bcc-anons', first=True).text.strip()
            ))
        return res
