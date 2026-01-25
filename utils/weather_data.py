import logging
import aiohttp
import asyncio

logger = logging.getLogger(__name__)

# Retry/backoff settings
MAX_RETRIES = 5
BACKOFF_BASE = 2  # exponential backoff base seconds


async def fetch_QuestWeatherStation_data(session, start_timestamp, end_timestamp):
    """
    Fetch data from the Quest Weather Station API between start_timestamp and end_timestamp.
    Retries on 429 (rate limiting), 5xx server errors, and network issues.
    Returns parsed JSON dict or {} on failure.
    """
    from config import Config  # Import within function to avoid circular imports
    url = (
        f"https://api.weatherlink.com/v2/historic/{Config.STATION_ID}"
        f"?api-key={Config.API_KEY}&start-timestamp={start_timestamp}&end-timestamp={end_timestamp}"
    )
    headers = {"X-Api-Secret": Config.API_SECRET}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                        logger.debug(
                            f"Successfully fetched data for timestamps {start_timestamp} to {end_timestamp}"
                        )
                        return data if isinstance(data, dict) else {}
                    except Exception as e:
                        logger.error(f"Failed to parse JSON from Quest API: {e}")
                        return {}

                elif response.status == 429:
                    wait = BACKOFF_BASE ** attempt
                    logger.warning(
                        f"Rate limited (429) from Quest API for {start_timestamp}–{end_timestamp}, "
                        f"retry {attempt}/{MAX_RETRIES} in {wait}s"
                    )
                    await asyncio.sleep(wait)

                elif 500 <= response.status < 600:
                    wait = BACKOFF_BASE ** attempt
                    logger.warning(
                        f"Server error {response.status} from Quest API for {start_timestamp}–{end_timestamp}, "
                        f"retry {attempt}/{MAX_RETRIES} in {wait}s"
                    )
                    await asyncio.sleep(wait)

                else:
                    text = await response.text()
                    logger.error(
                        f"Quest API returned {response.status} for {start_timestamp}–{end_timestamp}: {text}"
                    )
                    return {}

        except aiohttp.ClientError as e:
            wait = BACKOFF_BASE ** attempt
            logger.warning(
                f"Network error contacting Quest API for {start_timestamp}–{end_timestamp} ({e}), "
                f"retry {attempt}/{MAX_RETRIES} in {wait}s"
            )
            await asyncio.sleep(wait)

    logger.error(
        f"Quest API failed after {MAX_RETRIES} attempts "
        f"(start={start_timestamp}, end={end_timestamp})"
    )
    return {}


def generate_timestamps(start_date, end_date):
    """
    Yield (start_ts, end_ts) pairs for each day between start_date and end_date.
    """
    from datetime import timedelta

    current_date = start_date
    while current_date < end_date:
        yield int(current_date.timestamp()), int(
            (current_date + timedelta(days=1)).timestamp()
        )
        current_date += timedelta(days=1)
