async def main():
    from . import const
    from .catalog import CatalogDownloader, CatalogScrapper

    catalog = CatalogDownloader(const.HTML_ENCODING, const.PARALLEL_CONNECTIONS, const.RETRY_AFTER)
    catalog_scrapper = CatalogScrapper(catalog)

    producer = asyncio.create_task(catalog.run())
    consumer = asyncio.create_task(catalog_scrapper.run())
    await asyncio.wait([producer, consumer])

    from .pages import MissingPageDownloader, WatchPageScrapper

    pages = MissingPageDownloader(const.HTML_ENCODING, const.PARALLEL_CONNECTIONS, const.RETRY_AFTER)
    pages_scrapper = WatchPageScrapper(pages)

    producer = asyncio.create_task(pages.run())
    consumer = asyncio.create_task(pages_scrapper.run())
    await asyncio.wait([producer, consumer])


if __name__ == '__main__':
    import asyncio
    from ray.experimental import async_api as ray_async
    import ray

    ray.init(memory=1 / 2 * 1024 * 1024 * 1024, object_store_memory=1 / 2 * 1024 * 1024 * 1024)
    ray_async.init()
    asyncio.get_event_loop().run_until_complete(main())
