import asyncio
from collections import defaultdict
from typing import Any, AsyncGenerator, Dict, Optional, Tuple

import aiohttp
from tqdm import tqdm

from . import logger


# TODO: Add queue for urls

class BaseDownloader:
    BASE_URL: str = None

    def __init__(self, encoding: str, connections: int, retry_after: float, queue_maxsize=None):
        self.encoding = encoding
        self.connections = connections
        self.retry_after = retry_after
        self.context: Dict[str, Dict[str, Any]] = defaultdict(dict)

        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.queue: asyncio.Queue[Tuple[
            Optional[Dict[str, Any]],
            str
        ]] = asyncio.Queue(maxsize=queue_maxsize or 0, loop=self.loop)
        self._sem = asyncio.Semaphore(self.connections)
        self.tqdm: Optional[tqdm] = None

    @property
    async def urls(self) -> AsyncGenerator[str, Any]:
        yield self.BASE_URL

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
                        await asyncio.sleep(self.retry_after)
                        await self.fetch(url, **kwargs)
                        return None, None
                    return response.status, await response.read()

    async def run(self):
        futures = [
            self._run(url)
            async for url in self.urls
        ]
        self.tqdm = tqdm(desc='Dwnld', total=len(futures), dynamic_ncols=True)
        await asyncio.wait(futures)
        await self.queue.put((None, StopIteration()))

    async def _run(self, url):
        status, html = await self.fetch(url)
        if status is None:
            return
        self.tqdm.update(1)
        if status == 301:
            return
        html = html.decode(self.encoding, errors='replace')
        if status == 200 and html:
            await self.queue.put((self.context.get(url, None), html))
        else:
            logger.warn(f"STATUS => {status}")
            logger.warn(html)
