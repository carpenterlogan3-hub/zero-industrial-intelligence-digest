"""BR_01_deduplicate_articles.py

Read all existing URLs from Google Sheets 'Raw Feed Items' tab column D via
REUSABLE_sheets_data_layer. Load into Python set for O(1) lookup. Filter fetched
articles to new-only (URL not in set).

Exceptions:
    BE-01: All articles are duplicates (0 new) → log informational, skip store step.
"""

import logging
from typing import Dict, List

from src.reusable.sheets_data_layer import search_column

logger = logging.getLogger(__name__)

_TAB_NAME = "Raw Feed Items"
_URL_COLUMN = "url"


def deduplicate_articles(articles: List[Dict]) -> List[Dict]:
    """Return only articles whose URL is not already in the Sheets tab.

    Fetches the full URL column from 'Raw Feed Items' tab D once, builds a set,
    then filters the incoming articles list.

    Returns an empty list (with informational log) if all articles are duplicates.
    """
    existing_urls: set = set()

    try:
        raw_urls = search_column(_TAB_NAME, _URL_COLUMN)
        existing_urls = {u.strip() for u in raw_urls if u and u.strip()}
        logger.info("Loaded %d existing URLs from '%s' tab.", len(existing_urls), _TAB_NAME)
    except Exception as exc:
        # If we can't read the sheet, treat as empty to avoid data loss — let
        # store_raw_articles handle auth errors on write.
        logger.warning(
            "Could not read existing URLs from Sheets (will treat as empty): %s", exc
        )

    new_articles = [a for a in articles if a.get("url", "").strip() not in existing_urls]
    duplicate_count = len(articles) - len(new_articles)

    if duplicate_count:
        logger.info("Filtered %d duplicate article(s).", duplicate_count)

    # BE-01: informational — pipeline continues but store step should be skipped
    if not new_articles:
        logger.info(
            "BE-01: All %d fetched article(s) are duplicates. Nothing new to store.",
            len(articles),
        )

    return new_articles
