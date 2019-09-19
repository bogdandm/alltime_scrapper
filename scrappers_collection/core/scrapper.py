import asyncio
import multiprocessing
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Type

import ray
from ray.experimental import async_api as ray_async
from requests_html import HTML
from tqdm import tqdm

from .downloader import BaseDownloader
from .models import BaseSqliteModel
from ..const import TERMINAL_WIDTH


@ray.remote
def remote_process_html(cls: Type['BaseScrapper'], html: str, context: Optional[Dict[str, Any]] = None) \
        -> List[BaseSqliteModel]:
    return cls.process_html(html, context=context)


CPU_COUNT = multiprocessing.cpu_count()


class BaseScrapper(metaclass=ABCMeta):
    def __init__(self, downloader: BaseDownloader, use_ray=True, parallel_tasks=CPU_COUNT * 2):
        self.downloader = downloader
        self.use_ray = use_ray
        self.tqdm: Optional[tqdm] = None
        self._sem = asyncio.Semaphore(parallel_tasks)

    async def run(self):
        self.tqdm = tqdm(desc='Parse', total=0, ncols=TERMINAL_WIDTH, unit='items', smoothing=.05)
        tasks = []
        while True:
            self.tqdm.total = self.downloader.downloaded
            self.tqdm.update(0)

            await self._sem.acquire()
            context, html = await self.downloader.results_queue.get()
            if isinstance(html, StopIteration):
                break

            tasks.append(asyncio.create_task(self._run(context, html)))

        models: Tuple[Type[BaseSqliteModel]] = await asyncio.gather(*tasks)
        model = next(iter(models), None)
        if model:
            await model.drop_duplicates()

    async def _run(self, context, html):
        if self.use_ray:
            models: List[BaseSqliteModel] = await ray_async.as_future(remote_process_html.remote(
                type(self), html, context
            ))
        else:
            models: List[BaseSqliteModel] = self.process_html(html, context)
        model = type(next(iter(models), None))
        if model is type(None):
            return None

        await model.bulk_save(*models)

        self.downloader.results_queue.task_done()
        self._sem.release()
        self.tqdm.update(1)

        return model

    @classmethod
    @abstractmethod
    def process_html(cls, html: str, context: Optional[Dict[str, Any]] = None) -> List[BaseSqliteModel]:
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
