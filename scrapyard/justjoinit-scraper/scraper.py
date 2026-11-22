import asyncio
import logging
import os
import time

import httpx

from mapper import map_offers
from rabbitmq import publish_offers

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 30
PAGE_SIZE = 100
# The API rejects requests where from + itemsCount exceeds this
MAX_ITEMS = 10_000


async def start_scraping() -> int:
    started = time.monotonic()
    api_url = os.environ["API_URL"]

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
        first_page = await fetch_page(client, api_url, 0)
        offers = list(first_page["data"])
        total_items = min(first_page["meta"]["totalItems"], MAX_ITEMS)

        pages = await asyncio.gather(
            *(
                fetch_page(client, api_url, offset)
                for offset in range(PAGE_SIZE, total_items, PAGE_SIZE)
            )
        )
        for page in pages:
            offers.extend(page.get("data") or [])

    if not offers:
        logger.warning("JustJoinIt returned no job postings.")
        return 0

    await publish_offers(map_offers(offers))

    elapsed_ms = int((time.monotonic() - started) * 1000)
    logger.info("Fetched %d job ads from JustJoinIt in %d ms", len(offers), elapsed_ms)
    return len(offers)


async def fetch_page(client: httpx.AsyncClient, api_url: str, offset: int) -> dict:
    items_count = min(PAGE_SIZE, MAX_ITEMS - offset)
    response = await client.get(
        api_url, params={"from": offset, "itemsCount": items_count}
    )
    response.raise_for_status()
    return response.json()
