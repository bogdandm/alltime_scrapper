import asyncio
from abc import ABCMeta, abstractmethod
from typing import Optional, List, Type, Tuple

import ray
from ray.experimental import async_api as ray_async
from requests_html import HTML
from tqdm import tqdm

from const import HTML_ENCODING, TERMINAL_WIDTH
from .models import BaseSqliteModel, CatalogWatch
from .web_page_downloader import BaseDownloader


@ray.remote
def remote_process_html(cls: Type['BaseScrapper'], html: str) -> List[BaseSqliteModel]:
    return cls.process_html(html)


class BaseScrapper(metaclass=ABCMeta):
    def __init__(self, downloader: BaseDownloader):
        self.downloader = downloader
        self.tqdm: Optional[tqdm] = None
        self.done: int = 0

    async def run(self):
        # TODO: Rewrite to `ray`
        self.tqdm = tqdm(desc='Parse', total=0, ncols=TERMINAL_WIDTH, unit='items')
        tasks = []
        while True:
            html = await self.downloader.queue.get()
            if isinstance(html, StopIteration):
                break

            tasks.append(asyncio.create_task(self._run(html)))
            self.downloader.queue.task_done()

        models: Tuple[Type[BaseSqliteModel]] = await asyncio.gather(*tasks)
        model = next(iter(models), None)
        if model:
            await model.post_process()

    async def _run(self, html):
        models: List[BaseSqliteModel] = await ray_async.as_future(remote_process_html.remote(type(self), html))
        model = type(models[0])

        await model.bulk_save(*models)
        self.tqdm.update(len(models))

        self.done += 1
        return model

    @classmethod
    @abstractmethod
    def process_html(cls, html: str) -> List[BaseSqliteModel]:
        yield None

    @staticmethod
    def process_number(x: str):
        return int("".join(filter(
            lambda ch: '0' <= ch <= '9',
            x
        )))

    @staticmethod
    def _process_html(html: str) -> HTML:
        return HTML(html=html.encode(HTML_ENCODING), default_encoding=HTML_ENCODING)


class CatalogScrapper(BaseScrapper):
    @classmethod
    def process_html(cls, html: str) -> List[CatalogWatch]:
        html_doc: HTML = cls._process_html(html)
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
