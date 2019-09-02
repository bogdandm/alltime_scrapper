async def main():
    from .web_page_downloader import Catalog
    from .scrapper import CatalogScrapper

    catalog = Catalog()
    catalog_scrapper = CatalogScrapper(catalog)

    catalog_producer = asyncio.create_task(catalog.run())
    catalog_consumer = asyncio.create_task(catalog_scrapper.run())
    await asyncio.wait([catalog_producer, catalog_consumer])


if __name__ == '__main__':
    import asyncio
    from ray.experimental import async_api as ray_async
    import ray

    ray.init(object_store_memory=1 * 1024 * 1024 * 1024)
    ray_async.init()
    asyncio.get_event_loop().run_until_complete(main())
