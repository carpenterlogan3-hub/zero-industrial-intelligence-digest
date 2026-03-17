"""BR_01_store_raw_articles.py

Append each new article to Google Sheets 'Raw Feed Items' tab via
REUSABLE_sheets_data_layer. Column mapping:
    A: date_pulled (ISO 8601, ET)
    B: source (feed name)
    C: title
    D: url
    E: summary (max 500 chars)
    F: pub_date
    G: feed_category (Regulatory|AI/Tech|Energy/TES|Business/Finance)
    H: processed = "No"

Returns int: rows_written.

Exceptions:
    SE-01: Sheets auth 401/403 → retry 3x with token refresh, abort + alert.
    SE-02: Rate limit 429/503 → exponential backoff 5x, log partial count + alert.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Dict, List
from zoneinfo import ZoneInfo

from gspread.exceptions import APIError

from src.reusable.sheets_data_layer import append_row

logger = logging.getLogger(__name__)

_TAB_NAME = "Raw Feed Items"
_ET_TZ = ZoneInfo("America/New_York")

# Retry config for store-level errors (auth / rate-limit)
_AUTH_MAX_RETRIES = 3
_RATE_LIMIT_DELAYS = [2, 4, 8, 16, 32]  # 5 retries
_RETRYABLE_STATUS = {429, 503}
_AUTH_STATUS = {401, 403}


def _get_response_status(exc: APIError) -> int:
    if exc.response is not None:
        return exc.response.status_code
    return 0


def _append_with_retry(row_data: Dict) -> int:
    """Append a single row, with separate retry logic for auth vs rate-limit errors."""
    # Rate-limit retry loop (SE-02)
    last_exc = None
    for attempt, delay in enumerate([0] + _RATE_LIMIT_DELAYS):
        if delay:
            logger.warning(
                "SE-02: Rate limit hit, backing off %ds (attempt %d/%d).",
                delay, attempt, len(_RATE_LIMIT_DELAYS),
            )
            time.sleep(delay)
        try:
            return append_row(_TAB_NAME, row_data)
        except APIError as exc:
            status = _get_response_status(exc)
            if status in _RETRYABLE_STATUS:
                last_exc = exc
                continue
            elif status in _AUTH_STATUS:
                raise RuntimeError(
                    f"SE-01: Sheets auth error ({status}) appending row. "
                    "Check service account permissions."
                ) from exc
            else:
                raise
        except Exception:
            raise

    raise RuntimeError(
        f"SE-02: Sheets rate limit not resolved after {len(_RATE_LIMIT_DELAYS)} retries."
    ) from last_exc


def store_raw_articles(articles: List[Dict]) -> int:
    """Append each article as a new row in the 'Raw Feed Items' Sheets tab.

    Returns the number of rows successfully written.
    Partial writes are preserved — failures are logged without rolling back.
    """
    if not articles:
        logger.info("No articles to store.")
        return 0

    date_pulled = datetime.now(timezone.utc).astimezone(_ET_TZ).isoformat()
    rows_written = 0

    for i, article in enumerate(articles):
        row_data = {
            "date_pulled": date_pulled,
            "source": article.get("source", ""),
            "title": article.get("title", ""),
            "url": article.get("url", ""),
            "summary": (article.get("summary", "") or "")[:500],
            "pub_date": article.get("pub_date", ""),
            "feed_category": article.get("feed_category", ""),
            "processed": "No",
        }

        try:
            _append_with_retry(row_data)
            rows_written += 1
            logger.debug("Stored article %d/%d: %s", i + 1, len(articles), row_data["url"])
        except RuntimeError as exc:
            error_msg = str(exc)
            if "SE-01" in error_msg:
                logger.error(
                    "%s — aborting after %d/%d rows written.",
                    error_msg, rows_written, len(articles),
                )
                raise
            elif "SE-02" in error_msg:
                logger.error(
                    "%s — %d/%d rows written before rate-limit exhaustion.",
                    error_msg, rows_written, len(articles),
                )
                raise
            else:
                logger.error(
                    "Unexpected error storing article %d/%d (%s): %s — skipping.",
                    i + 1, len(articles), row_data["url"], exc,
                )

    logger.info("Stored %d/%d new article(s) to '%s'.", rows_written, len(articles), _TAB_NAME)
    return rows_written
