import asyncio
from abc import ABCMeta, abstractmethod
from typing import List, Optional, Tuple, Type

import ray
from ray.experimental import async_api as ray_async
from requests_html import HTML
from tqdm import tqdm

from .downloader import BaseDownloader
from .models import BaseSqliteModel
from ..const import TERMINAL_WIDTH


@ray.remote
def remote_process_html(cls: Type['BaseScrapper'], html: str) -> List[BaseSqliteModel]:
    return cls.process_html(html)


class BaseScrapper(metaclass=ABCMeta):
    def __init__(self, downloader: BaseDownloader):
        self.downloader = downloader
        self.tqdm: Optional[tqdm] = None
        self.done: int = 0

    async def run(self):
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
            await model.drop_duplicates()

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
        return []

    @staticmethod
    def process_number(x: str):
        return int("".join(filter(
            lambda ch: '0' <= ch <= '9',
            x
        )))

    @staticmethod
    def parse_html(html: str, encoding: str = 'utf-8') -> HTML:
        return HTML(html=html.encode(encoding), default_encoding=encoding)
