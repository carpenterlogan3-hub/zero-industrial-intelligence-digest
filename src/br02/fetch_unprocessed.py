"""BR_02_fetch_unprocessed.py

Query Google Sheets 'Raw Feed Items' tab: all rows where column H = 'No'.
Returns list of dicts with all 8 columns + _row_number for later update.

Exceptions:
    SE-01: Sheets 401/403/404 → retry 3x, abort + alert.
    BE-01: Zero unprocessed rows → log informational, skip to BR_03.
"""

import logging
import time
from typing import Dict, List

from gspread.exceptions import APIError

from src.reusable.sheets_data_layer import read_rows

logger = logging.getLogger(__name__)

_TAB_NAME = "Raw Feed Items"
_PROCESSED_COLUMN = "processed"
_UNPROCESSED_VALUE = "No"
_AUTH_STATUS = {401, 403, 404}
_AUTH_MAX_RETRIES = 3
_AUTH_RETRY_DELAY = 2


def fetch_unprocessed_articles() -> List[Dict]:
    """Return all rows from 'Raw Feed Items' where processed = 'No'.

    Each dict contains all 8 columns plus _row_number.
    Returns an empty list (with informational log) if none are found.
    Retries up to 3x on auth/not-found errors before aborting.
    """
    last_exc = None
    for attempt in range(1, _AUTH_MAX_RETRIES + 1):
        try:
            rows = read_rows(_TAB_NAME, filter_column=_PROCESSED_COLUMN, filter_value=_UNPROCESSED_VALUE)
            break
        except APIError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status in _AUTH_STATUS:
                logger.warning(
                    "SE-01: Sheets error %d on attempt %d/%d fetching unprocessed rows.",
                    status, attempt, _AUTH_MAX_RETRIES,
                )
                last_exc = exc
                if attempt < _AUTH_MAX_RETRIES:
                    time.sleep(_AUTH_RETRY_DELAY * attempt)
            else:
                raise
        except Exception as exc:
            raise
    else:
        raise RuntimeError(
            f"SE-01: Failed to fetch unprocessed rows after {_AUTH_MAX_RETRIES} retries. "
            f"Last error: {last_exc}"
        ) from last_exc

    # BE-01: informational — caller should skip store step
    if not rows:
        logger.info("BE-01: Zero unprocessed rows in '%s' tab. Nothing to classify.", _TAB_NAME)
        return []

    logger.info("Fetched %d unprocessed article(s) from '%s'.", len(rows), _TAB_NAME)
    return rows
