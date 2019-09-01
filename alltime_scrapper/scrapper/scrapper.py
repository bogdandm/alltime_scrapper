from abc import ABCMeta, abstractmethod
from typing import Any, Generator, Optional

from requests_html import HTML
from tqdm import tqdm

from const import HTML_ENCODING, TERMINAL_WIDTH
from .models import BaseSqliteModel, CatalogWatch
from .web_page_downloader import BaseDownloader


class BaseScrapper(metaclass=ABCMeta):
    def __init__(self, downloader: BaseDownloader):
        self.downloader = downloader
        self.tqdm: Optional[tqdm] = None

    async def run(self):
        # TODO: Rewrite to `ray`
        self.tqdm = tqdm(total=0, ncols=TERMINAL_WIDTH)
        models = []
        while True:
            html = await self.downloader.queue.get()
            self.tqdm.refresh()
            if isinstance(html, StopIteration):
                break

            models_current = list(self.process_html(html))
            models.extend(models_current)
            self.tqdm.total += len(models_current)
            self.tqdm.refresh()
            self.downloader.queue.task_done()

        batch_size = 50
        model = type(models[0])
        for i in range(0, len(models), batch_size):
            batch = models[i:i + batch_size]
            await model.bulk_save(*batch)
            self.tqdm.update(len(batch))
        await model.post_process()

    @abstractmethod
    def process_html(self, html: str) -> Generator[BaseSqliteModel, Any, Any]:
        yield None

    def process_number(self, x: str):
        return int("".join(filter(
            lambda ch: '0' <= ch <= '9',
            x
        )))

    def _process_html(self, html: str) -> HTML:
        return HTML(html=html.encode(HTML_ENCODING), default_encoding=HTML_ENCODING)


class CatalogScrapper(BaseScrapper):
    def process_html(self, html: str) -> Generator[CatalogWatch, Any, Any]:
        html_doc: HTML = self._process_html(html)
        for card in html_doc.find('.bcc-post'):
            image = card.find('.bcc-image .first_image', first=True)
            price_new, *price_old = card.find('.bcc-price', first=True).find('li')
            price_old = price_old and price_old[0] or None
            if price_old:
                price_old = self.process_number(price_old.text)

            yield CatalogWatch(
                name=card.find('.bcc-title', first=True).text.strip().split('\n')[0].strip(),
                href=image.attrs['data-href'],
                image_href=image.find('img', first=True).attrs['src'],
                price=self.process_number(price_new.text),
                price_old=price_old,
                text=card.find('.bcc-anons', first=True).text.strip()
            )
