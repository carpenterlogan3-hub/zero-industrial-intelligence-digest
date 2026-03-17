"""BR_02_mark_processed.py

Update Google Sheets 'Raw Feed Items' tab: set column H from 'No' to 'Yes'
for each article successfully classified AND stored. Uses _row_number from
fetch_unprocessed output.

Exceptions:
    SE-01: Update fails → log error, continue. Article gets reclassified next run
           (idempotent, ~$0.001 cost per missed update).
"""

import logging
from typing import Dict, List

from src.reusable.sheets_data_layer import update_cell

logger = logging.getLogger(__name__)

_TAB_NAME = "Raw Feed Items"
_PROCESSED_COLUMN = "processed"
_PROCESSED_VALUE = "Yes"


def mark_articles_processed(articles: List[Dict]) -> int:
    """Set processed = 'Yes' for each article in 'Raw Feed Items'.

    Uses _row_number from each article dict to target the correct row.
    Failures are logged and skipped — the article will be reclassified on the next run
    (idempotent; negligible cost).

    Returns the number of rows successfully updated.
    """
    if not articles:
        logger.info("No articles to mark as processed.")
        return 0

    updated = 0
    for article in articles:
        row_number = article.get("_row_number")
        url = article.get("url", "?")

        if not row_number:
            logger.error(
                "SE-01: Missing _row_number for article '%s' — cannot mark processed.", url
            )
            continue

        try:
            update_cell(_TAB_NAME, row_number, _PROCESSED_COLUMN, _PROCESSED_VALUE)
            updated += 1
            logger.debug("Marked row %d as processed (%s).", row_number, url)
        except Exception as exc:
            # SE-01: log and continue — idempotent, article reclassified next run
            logger.error(
                "SE-01: Failed to mark row %d as processed (%s): %s — will retry next run.",
                row_number, url, exc,
            )

    if updated < len(articles):
        logger.warning(
            "%d/%d article(s) could not be marked processed — will be reclassified next run.",
            len(articles) - updated, len(articles),
        )

    logger.info("Marked %d/%d article(s) as processed in '%s'.", updated, len(articles), _TAB_NAME)
    return updated
