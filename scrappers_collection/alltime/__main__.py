async def main():
    from . import const
    from .catalog import CatalogDownloader, CatalogScrapper

    catalog = CatalogDownloader(
        encoding=const.HTML_ENCODING,
        parallel_downloads=const.CATALOG_PARALLEL_CONNECTIONS,
        retry_after=const.RETRY_AFTER,
        results_queue_maxsize=const.CATALOG_PARSE_QUEUE,
    )
    catalog_scrapper = CatalogScrapper(catalog)

    producer = asyncio.create_task(catalog.run())
    consumer = asyncio.create_task(catalog_scrapper.run())
    await asyncio.wait([producer, consumer])

    from .pages import MissingPageDownloader, WatchPageScrapper

    pages = MissingPageDownloader(
        encoding=const.HTML_ENCODING,
        parallel_downloads=const.PAGES_PARALLEL_CONNECTIONS,
        retry_after=const.RETRY_AFTER,
        results_queue_maxsize=const.PAGES_PARSE_QUEUE,
    )
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
