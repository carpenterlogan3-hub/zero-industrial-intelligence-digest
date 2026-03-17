"""BR_04_compile_summary.py

Aggregate runtime counters into a summary dict with exactly 15 keys:
    pipeline_run_date, pipeline_start_time, pipeline_end_time,
    feeds_attempted, feeds_errored, articles_fetched, articles_new,
    articles_classified, articles_classification_errors,
    digests_generated, emails_sent, emails_failed,
    slack_messages_sent, total_errors, error_highlights (first 5, 200 chars each).
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def compile_summary(
    pipeline_start_time: datetime,
    pipeline_end_time: datetime,
    feeds_attempted: int = 0,
    feeds_errored: int = 0,
    articles_fetched: int = 0,
    articles_new: int = 0,
    articles_classified: int = 0,
    articles_classification_errors: int = 0,
    digests_generated: int = 0,
    emails_sent: int = 0,
    emails_failed: int = 0,
    slack_messages_sent: int = 0,
    errors: List[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the 15-key pipeline summary dict.

    Args:
        pipeline_start_time: timezone-aware datetime when pipeline started.
        pipeline_end_time: timezone-aware datetime when pipeline ended.
        errors: list of error record dicts, each with at least 'error_message'.

    Returns exactly 15 keys.
    """
    errors = errors or []
    total_errors = len(errors)

    # First 5 error messages, each truncated to 200 chars
    error_highlights = [
        str(e.get("error_message", ""))[:200]
        for e in errors[:5]
    ]

    summary = {
        "pipeline_run_date": pipeline_start_time.strftime("%Y-%m-%d"),
        "pipeline_start_time": pipeline_start_time.isoformat(),
        "pipeline_end_time": pipeline_end_time.isoformat(),
        "feeds_attempted": feeds_attempted,
        "feeds_errored": feeds_errored,
        "articles_fetched": articles_fetched,
        "articles_new": articles_new,
        "articles_classified": articles_classified,
        "articles_classification_errors": articles_classification_errors,
        "digests_generated": digests_generated,
        "emails_sent": emails_sent,
        "emails_failed": emails_failed,
        "slack_messages_sent": slack_messages_sent,
        "total_errors": total_errors,
        "error_highlights": error_highlights,
    }

    assert len(summary) == 15, f"compile_summary must return exactly 15 keys, got {len(summary)}"

    logger.info(
        "Pipeline summary: %d feeds, %d articles fetched, %d new, %d classified, "
        "%d digests, %d emails sent, %d errors.",
        feeds_attempted, articles_fetched, articles_new, articles_classified,
        digests_generated, emails_sent, total_errors,
    )
    return summary
