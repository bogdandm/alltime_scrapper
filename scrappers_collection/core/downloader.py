import asyncio
from collections import defaultdict
from typing import Any, AsyncGenerator, Dict, Optional, Tuple

import aiohttp
from tqdm import tqdm

from . import logger


class BaseDownloader:
    BASE_URL: str = None

    def __init__(self, encoding: str, connections: int, retry_after: float, queue_maxsize=100):
        self.encoding = encoding
        self.connections = connections
        self.retry_after = retry_after
        self.context: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self.queue_maxsize = queue_maxsize

        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.queue: asyncio.Queue[Tuple[
            Optional[Dict[str, Any]],
            str
        ]] = asyncio.Queue(maxsize=self.queue_maxsize, loop=self.loop)
        self._sem = asyncio.Semaphore(self.connections)

        self.tqdm: Optional[tqdm] = None
        self.start_download: int = 0

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
        urls = [url async for url in self.urls]
        self.tqdm = tqdm(desc='Dwnld', total=len(urls), dynamic_ncols=True)
        batch_size = self.connections * 2
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            self.start_download += len(batch)
            await asyncio.wait([self._run(url) for url in batch])
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
