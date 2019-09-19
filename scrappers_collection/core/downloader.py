import asyncio
from collections import defaultdict
from typing import Any, AsyncGenerator, Dict, Optional, Tuple

import aiohttp
from tqdm import tqdm

from . import logger


class BaseDownloader:
    BASE_URL: str = None

    def __init__(self, encoding: str, parallel_downloads: int, retry_after: float, results_queue_maxsize: int):
        self.encoding = encoding
        self.retry_after = retry_after
        self.context: Dict[str, Dict[str, Any]] = defaultdict(dict)

        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.download_queue: asyncio.Queue[str] = asyncio.Queue(loop=self.loop)
        self.results_queue: asyncio.Queue[Tuple[
            Optional[Dict[str, Any]],
            str
        ]] = asyncio.Queue(maxsize=results_queue_maxsize, loop=self.loop)
        self._download_lock = asyncio.Semaphore(parallel_downloads, loop=self.loop)

        self.tqdm: Optional[tqdm] = None
        self.downloaded: int = 0

    @property
    async def urls(self) -> AsyncGenerator[str, Any]:
        yield self.BASE_URL

    @property
    def cookies(self) -> Dict[str, str]:
        return {}

    async def fetch(self, url: str, **kwargs) -> Tuple[Optional[int], Optional[bytes]]:
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
        for url in urls:
            await self.download_queue.put(url)
        self.tqdm = tqdm(desc='Dwnld', total=len(urls), dynamic_ncols=True, smoothing=.05)
        tasks = []
        while self.download_queue.qsize():
            await self._download_lock.acquire()
            url = await self.download_queue.get()
            tasks.append(asyncio.create_task(self._download(url)))
        await asyncio.wait(tasks)
        await self.results_queue.put((None, StopIteration()))

    async def _download(self, url):
        status, html = await self.fetch(url)
        if status is None:
            return
        self.tqdm.update(1)
        if status != 301:
            html = html.decode(self.encoding, errors='replace')
            if status == 200 and html:
                await self.results_queue.put((self.context.get(url, None), html))
                self.downloaded += 1
            else:
                logger.warn(f"STATUS => {status}")
                logger.warn(html)
        self._download_lock.release()
