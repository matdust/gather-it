import asyncio
import logging
import os
import time

import httpx

from mapper import map_offers
from rabbitmq import publish_offers

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 30
PAGE_DELAY_SECONDS = 0.3
# The search endpoint requires a filter body; empty filters return every offer
SEARCH_FILTERS = {
    "typesOfContractIds": [],
    "positionLevelIds": [],
    "cities": [],
    "workModeCodes": [],
    "onlyWithProjectDescription": False,
    "expectedTechnologies": [],
    "niceToHaveTechnologies": [],
    "excludedTechnologies": [],
    "regionsOfWorld": [],
    "keywords": [],
    "specializationsCodes": [],
    "isSupportingUkraine": False,
    "fromExternalLocations": False,
}


def http_headers() -> dict:
    # Cloudflare challenges requests that don't look like they come from a browser
    return {
        "User-Agent": os.environ["TP_USER_AGENT"],
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://theprotocol.it",
        "Referer": "https://theprotocol.it/",
        "Cache-Control": "no-cache",
        "X-Application-Name": "theprotocol-offers",
        "x-xsrf-token": os.environ["TP_XSRF_TOKEN"],
        "Cookie": os.environ["TP_COOKIE"],
    }


async def start_scraping() -> int:
    started = time.monotonic()
    api_url = os.environ["API_URL"]

    async with httpx.AsyncClient(
        headers=http_headers(), timeout=REQUEST_TIMEOUT_SECONDS
    ) as client:
        first_page = await fetch_page(client, api_url, 1)
        offers = list(first_page["offers"])
        total_pages = first_page["page"]["count"]

        for page_number in range(2, total_pages + 1):
            await asyncio.sleep(PAGE_DELAY_SECONDS)
            page = await fetch_page(client, api_url, page_number)
            offers.extend(page.get("offers") or [])

    if not offers:
        logger.warning("TheProtocolIt returned no job postings.")
        return 0

    await publish_offers(map_offers(offers))

    elapsed_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        "Fetched %d job ads from TheProtocolIt in %d ms", len(offers), elapsed_ms
    )
    return len(offers)


async def fetch_page(client: httpx.AsyncClient, api_url: str, page_number: int) -> dict:
    response = await client.post(f"{api_url}{page_number}", json=SEARCH_FILTERS)
    response.raise_for_status()
    return response.json()
