import asyncio
import logging
import os
from contextlib import asynccontextmanager

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

import scraper

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

SCRAPER_ID = "theprotocolit-scraper-python"
SCRAPE_INTERVAL_SECONDS = 36000
REGISTRATION_ATTEMPTS = 5
REGISTRATION_RETRY_DELAY_SECONDS = 15


async def register_scraper() -> bool:
    scheduler_url = os.environ["SCHEDULER_URL"]
    payload = {
        "id": SCRAPER_ID,
        "endpoint": os.environ["SCRAPER_ENDPOINT"],
        "interval": SCRAPE_INTERVAL_SECONDS,
    }
    async with httpx.AsyncClient() as client:
        response = await client.post(scheduler_url, json=payload)
    if response.is_success or response.status_code == 409:
        logger.info("Registered with scheduler (status %d)", response.status_code)
        return True
    logger.warning("Scheduler registration returned status %d", response.status_code)
    return False


async def handle_initial_registration() -> None:
    for attempt in range(1, REGISTRATION_ATTEMPTS + 1):
        try:
            if await register_scraper():
                return
        except httpx.HTTPError as error:
            logger.warning(
                "Scheduler registration attempt %d/%d failed: %s",
                attempt,
                REGISTRATION_ATTEMPTS,
                error,
            )
        await asyncio.sleep(REGISTRATION_RETRY_DELAY_SECONDS)
    logger.error(
        "Could not register with scheduler after %d attempts", REGISTRATION_ATTEMPTS
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    registration = asyncio.create_task(handle_initial_registration())
    yield
    registration.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/healthcheck")
async def healthcheck() -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.post("/register")
async def register() -> PlainTextResponse:
    try:
        registered = await register_scraper()
    except httpx.HTTPError:
        logger.exception("Scheduler registration failed")
        return PlainTextResponse("Could not reach scheduler.", status_code=502)
    if not registered:
        return PlainTextResponse("Scheduler rejected registration.", status_code=502)
    return PlainTextResponse("Registered with scheduler.")


@app.get("/scrap")
async def scrap() -> PlainTextResponse:
    try:
        offers_count = await scraper.start_scraping()
    except httpx.HTTPError:
        logger.exception("Fetching offers from TheProtocolIt failed")
        return PlainTextResponse(
            "Fetching offers from TheProtocolIt failed.", status_code=502
        )
    except Exception:
        logger.exception("Scraping failed")
        return PlainTextResponse("Scraping failed.", status_code=500)
    return PlainTextResponse(
        f"Scraping completed successfully. Got {offers_count} offers."
    )
