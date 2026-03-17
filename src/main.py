"""main.py — Pipeline orchestrator.

Executes BR_01 → BR_02 → BR_04 in sequence.
BR_03 (digest generation and email delivery) is disabled — pipeline only populates Sheets.
Daily 6:00 AM ET via GitHub Actions cron (0 10 * * *).

Usage: python -m src.main
"""
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

# Configure logging before any module imports that log at import time
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# BR_01
from src.br01.rss_feed_config import load_feed_config
from src.br01.fetch_rss_feeds import fetch_all_feeds
from src.br01.deduplicate_articles import deduplicate_articles
from src.br01.store_raw_articles import store_raw_articles

# BR_02
from src.br02.fetch_unprocessed import fetch_unprocessed_articles
from src.br02.classify_article import classify_articles
from src.br02.store_classified import store_classified_articles
from src.br02.mark_processed import mark_articles_processed

# BR_04
from src.br04.compile_summary import compile_summary
from src.br04.log_errors import log_errors
from src.br04.send_completion_email import send_completion_email

_ET_TZ = ZoneInfo("America/New_York")
_ADMIN_EMAIL = "logan@grizinc.com"


def _err(module: str, error_type: str, message: str, affected: str = "N/A") -> Dict[str, Any]:
    """Create a standardised error record dict."""
    return {
        "module_name": module,
        "error_type": error_type,
        "error_message": message,
        "affected_item": affected,
    }


def run_pipeline() -> int:
    """Execute BR_01 → BR_02 → BR_04.

    Returns:
        0 — clean run (zero errors).
        1 — one or more errors occurred (pipeline continued where possible).
    """
    et = _ET_TZ
    pipeline_start = datetime.now(timezone.utc).astimezone(et)
    logger.info("=== Pipeline started: %s ===", pipeline_start.isoformat())
    print(f"\n{'#'*60}")
    print(f"PIPELINE START: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"{'#'*60}\n")

    errors: List[Dict[str, Any]] = []

    # ── Counters ──────────────────────────────────────────────────────────
    feeds_attempted = 0
    feeds_errored = 0
    articles_fetched = 0
    articles_new = 0
    articles_classified = 0
    articles_classification_errors = 0

    # ══════════════════════════════════════════════════════════════════════
    # BR_01 — Ingest RSS feeds
    # ══════════════════════════════════════════════════════════════════════
    print("── BR_01: RSS Feed Ingestion ──")
    logger.info("── BR_01: RSS Feed Ingestion ──")

    try:
        feed_list = load_feed_config()
        feeds_attempted = len(feed_list)
        print(f"  Loaded {feeds_attempted} feeds from config")
    except Exception as exc:
        msg = f"BR_01 config load failed — aborting pipeline: {exc}"
        logger.error(msg)
        errors.append(_err("BR_01_rss_feed_config.py", "SE", msg))
        _finish(pipeline_start, errors, feeds_attempted, feeds_errored,
                articles_fetched, articles_new, articles_classified,
                articles_classification_errors)
        return 1

    try:
        fetched_articles = fetch_all_feeds(feed_list)
        articles_fetched = len(fetched_articles)
        active_sources = {a["source"] for a in fetched_articles}
        feeds_errored = feeds_attempted - len(active_sources)
        print(f"  Fetched {articles_fetched} articles from {len(active_sources)}/{feeds_attempted} feeds")
    except Exception as exc:
        msg = f"BR_01 fetch_all_feeds unexpected failure: {exc}"
        logger.error(msg)
        errors.append(_err("BR_01_fetch_rss_feeds.py", "SE", msg))
        fetched_articles = []

    try:
        new_articles = deduplicate_articles(fetched_articles)
        articles_new = len(new_articles)
        print(f"  {articles_new} new articles after deduplication ({articles_fetched - articles_new} duplicates)")
    except Exception as exc:
        msg = f"BR_01 deduplication failed: {exc}"
        logger.error(msg)
        errors.append(_err("BR_01_deduplicate_articles.py", "SE", msg))
        new_articles = []

    if new_articles:
        try:
            store_raw_articles(new_articles)
            print(f"  Stored {articles_new} new articles to Raw Feed Items tab")
        except Exception as exc:
            msg = f"BR_01 store_raw_articles failed: {exc}"
            logger.error(msg)
            errors.append(_err("BR_01_store_raw_articles.py", "SE", msg))

    # ══════════════════════════════════════════════════════════════════════
    # BR_02 — Classify articles
    # ══════════════════════════════════════════════════════════════════════
    print("\n── BR_02: Classification ──")
    logger.info("── BR_02: Classification ──")
    classified_articles: list = []

    try:
        unprocessed = fetch_unprocessed_articles()
        print(f"  {len(unprocessed)} unprocessed articles to classify")
    except Exception as exc:
        msg = f"BR_02 fetch_unprocessed failed: {exc}"
        logger.error(msg)
        errors.append(_err("BR_02_fetch_unprocessed.py", "SE", msg))
        unprocessed = []

    if unprocessed:
        try:
            classified_articles = classify_articles(unprocessed)
            articles_classified = len(classified_articles)
            # articles_classification_errors includes both hard errors and SKIP/LOW
            articles_classification_errors = len(unprocessed) - articles_classified
        except RuntimeError as exc:
            msg = f"BR_02 classify_articles aborted (auth): {exc}"
            logger.error(msg)
            errors.append(_err("BR_02_classify_article.py", "SE", msg))
            classified_articles = []
            articles_classification_errors = len(unprocessed)
        except Exception as exc:
            msg = f"BR_02 classify_articles unexpected failure: {exc}"
            logger.error(msg)
            errors.append(_err("BR_02_classify_article.py", "SE", msg))
            classified_articles = []

    if classified_articles:
        print(f"\n── BR_02: Storing {len(classified_articles)} classified articles ──")
        try:
            stored_classified = store_classified_articles(classified_articles)
            print(f"  Stored {len(stored_classified)}/{len(classified_articles)} articles to Classified Items tab")
        except Exception as exc:
            msg = f"BR_02 store_classified failed: {exc}"
            logger.error(msg)
            errors.append(_err("BR_02_store_classified.py", "SE", msg))
            stored_classified = []

        if stored_classified:
            try:
                mark_articles_processed(stored_classified)
                print(f"  Marked {len(stored_classified)} articles as processed in Raw Feed Items tab")
            except Exception as exc:
                msg = f"BR_02 mark_processed failed: {exc}"
                logger.error(msg)
                errors.append(_err("BR_02_mark_processed.py", "SE", msg))

    # ══════════════════════════════════════════════════════════════════════
    # BR_04 — Summarise and log
    # ══════════════════════════════════════════════════════════════════════
    _finish(
        pipeline_start, errors,
        feeds_attempted, feeds_errored,
        articles_fetched, articles_new,
        articles_classified, articles_classification_errors,
    )

    exit_code = 0 if not errors else 1
    logger.info("=== Pipeline finished. Exit code: %d ===", exit_code)
    print(f"\n{'#'*60}")
    print(f"PIPELINE COMPLETE — exit code {exit_code} ({'CLEAN' if exit_code == 0 else 'ERRORS'})")
    print(f"{'#'*60}\n")
    return exit_code


def _finish(
    pipeline_start: datetime,
    errors: List[Dict[str, Any]],
    feeds_attempted: int,
    feeds_errored: int,
    articles_fetched: int,
    articles_new: int,
    articles_classified: int,
    articles_classification_errors: int,
) -> None:
    """Run BR_04: compile summary, log errors, send completion email."""
    logger.info("── BR_04: Summary & Notification ──")
    print("\n── BR_04: Summary ──")
    pipeline_end = datetime.now(timezone.utc).astimezone(_ET_TZ)

    summary = compile_summary(
        pipeline_start_time=pipeline_start,
        pipeline_end_time=pipeline_end,
        feeds_attempted=feeds_attempted,
        feeds_errored=feeds_errored,
        articles_fetched=articles_fetched,
        articles_new=articles_new,
        articles_classified=articles_classified,
        articles_classification_errors=articles_classification_errors,
        digests_generated=0,
        emails_sent=0,
        emails_failed=0,
        slack_messages_sent=0,
        errors=errors,
    )

    try:
        log_errors(errors, pipeline_run_date=summary["pipeline_run_date"])
    except Exception as exc:
        logger.error("BR_04 log_errors failed: %s", exc)

    try:
        send_completion_email(summary, admin_email=_ADMIN_EMAIL)
    except Exception as exc:
        logger.error("BR_04 send_completion_email failed: %s", exc)


if __name__ == "__main__":
    sys.exit(run_pipeline())
