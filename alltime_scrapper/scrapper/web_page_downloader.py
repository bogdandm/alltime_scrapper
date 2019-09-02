import asyncio
from typing import Dict, Iterable, Optional, Tuple

import aiohttp

from const import CATALOG_PAGE, CATALOG_PAGES, HTML_ENCODING, PARALLEL_CONNECTIONS, RETRY_AFTER
from . import logger


class BaseDownloader:
    BASE_URL = None

    def __init__(self):
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.queue: asyncio.Queue[str] = asyncio.Queue(loop=self.loop)
        self._sem = asyncio.Semaphore(PARALLEL_CONNECTIONS)

    @property
    def urls(self) -> Iterable[str]:
        return [self.BASE_URL]

    @property
    def cookies(self) -> Dict[str, str]:
        return {}

    async def fetch(self, url: str, **kwargs) -> Tuple[Optional[int], Optional[bytes]]:
        async with self._sem:
            logger.info(f"Fetch {url} {kwargs}")
            async with aiohttp.ClientSession(cookies=self.cookies) as session:
                async with session.get(url, allow_redirects=False, **kwargs) as response:
                    logger.info(f"[{response.status}] Fetch {url} {kwargs}")
                    if response.status == 503:
                        await asyncio.sleep(RETRY_AFTER)
                        await self.fetch(url, **kwargs)
                        return None, None
                    return response.status, await response.read()

    async def run(self):
        await asyncio.wait([
            self._run(url)
            for url in self.urls
        ])
        await self.queue.put(StopIteration())

    async def _run(self, url):
        status, html = await self.fetch(url)
        if status is None or status == 301:
            return
        html = html.decode(HTML_ENCODING, errors='replace')
        if status == 200 and html:
            await self.queue.put(html)
        else:
            logger.warn(f"STATUS => {status}")
            logger.warn(html)


class Catalog(BaseDownloader):
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
