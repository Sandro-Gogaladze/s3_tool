import json
import logging
from typing import Optional
from urllib.parse import quote as url_quote
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
    ),
}

_BASE_URL = "https://api.quotable.kurokeita.dev/api/quotes/random"


def get_quote(author: Optional[str] = None) -> dict:
    """Fetch a random quote, optionally filtered by *author*."""
    url = f"{_BASE_URL}?author={url_quote(author)}" if author else _BASE_URL
    logger.info("Fetching quote from %s", url)
    try:
        with urlopen(Request(url, data=None, headers=_HEADERS)) as response:
            result = json.loads(response.read().decode())
    except Exception:
        logger.exception("Error fetching quote")
        return {}
    logger.info("Quote fetched successfully")
    return result


def print_quote(data: dict):
    """Print a quote to stdout in a human-friendly format."""
    quote_body = data.get("quote", {})
    content = quote_body.get("content", "No content")
    author = quote_body.get("author", {}).get("name", "Unknown")
    print(content)
    print("---")
    print(author)
