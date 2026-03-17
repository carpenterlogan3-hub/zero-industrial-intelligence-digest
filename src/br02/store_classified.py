"""BR_02_store_classified.py — V2

Append enriched article to Google Sheets 'Classified Items' tab. Column mapping:
    A: title
    B: url
    C: source
    D: pub_date
    E: topic_category
    F: relevant_persons (comma-separated full names, e.g. "Ted Kniesche, William Price")
    G: importance (HIGH or MEDIUM only — SKIP items never reach this module)
    H: one_line_summary
    I: digest_date (YYYY-MM-DD, ET timezone, today)

V2 CHANGES:
- Column F is now "relevant_persons" (full names) instead of "relevant_roles"
- Only HIGH and MEDIUM articles are stored (SKIP filtered upstream)

NOTE: row_data includes both "relevant_persons" and "relevant_roles" keys so this
module works correctly whether the sheet header in column F is the old name or the
new name. Once the header is confirmed as "relevant_persons", the legacy key can
be removed.

Exceptions:
    SE-01: Sheets write fail → retry 3x, log as unwritten, continue.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Dict, List
from zoneinfo import ZoneInfo

from gspread.exceptions import APIError

from src.reusable.sheets_data_layer import append_row

logger = logging.getLogger(__name__)

_TAB_NAME = "Classified Items"
_ET_TZ = ZoneInfo("America/New_York")
_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 2


def _get_digest_date() -> str:
    return datetime.now(timezone.utc).astimezone(_ET_TZ).strftime("%Y-%m-%d")


def _append_with_retry(row_data: Dict) -> bool:
    """Attempt to append a row up to 3 times. Returns True on success, False on failure."""
    last_exc = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            append_row(_TAB_NAME, row_data)
            return True
        except APIError as exc:
            last_exc = exc
            status = exc.response.status_code if exc.response is not None else 0
            logger.warning(
                "SE-01: Sheets write error %d on attempt %d/%d for '%s'.",
                status, attempt, _MAX_RETRIES, row_data.get("url", "?"),
            )
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_BASE_DELAY * attempt)
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "SE-01: Unexpected write error on attempt %d/%d for '%s': %s",
                attempt, _MAX_RETRIES, row_data.get("url", "?"), exc,
            )
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_BASE_DELAY * attempt)

    logger.error(
        "SE-01: Failed to write '%s' after %d retries. Logging as unwritten. Last error: %s",
        row_data.get("url", "?"), _MAX_RETRIES, last_exc,
    )
    return False


def store_classified_articles(articles: List[Dict]) -> List[Dict]:
    """Append each HIGH/MEDIUM classified article to 'Classified Items' tab.

    Returns only the articles successfully written (preserves _row_number
    so mark_processed can update the source tab).
    """
    if not articles:
        logger.info("No classified articles to store.")
        return []

    digest_date = _get_digest_date()
    stored: List[Dict] = []

    for article in articles:
        # relevant_persons is a list of full name strings set by classify_article.py
        persons = article.get("relevant_persons", [])
        if isinstance(persons, list):
            persons_str = ", ".join(persons)
        else:
            persons_str = str(persons)

        # Debug: show exactly what is being written to column F
        url = article.get("url", "?")
        print(f"  [STORE] Column F (relevant_persons) for '{url[:60]}': '{persons_str}'")

        row_data = {
            "title": article.get("title", ""),
            "url": url,
            "source": article.get("source", ""),
            "pub_date": article.get("pub_date", ""),
            "topic_category": article.get("topic_category", ""),
            # Write under both keys so the value lands in column F regardless of
            # whether the sheet header is the old name ("relevant_roles") or the
            # new name ("relevant_persons"). append_row matches dict keys to headers.
            "relevant_persons": persons_str,
            "relevant_roles": persons_str,
            "importance": article.get("importance", ""),
            "one_line_summary": article.get("one_line_summary", ""),
            "digest_date": digest_date,
        }

        if _append_with_retry(row_data):
            stored.append(article)
            logger.debug("Stored: %s | F='%s'", url[:60], persons_str)

    unwritten = len(articles) - len(stored)
    if unwritten:
        logger.warning("%d article(s) could not be written to '%s'.", unwritten, _TAB_NAME)

    logger.info(
        "Stored %d/%d classified article(s) to '%s'.",
        len(stored), len(articles), _TAB_NAME,
    )
    return stored
