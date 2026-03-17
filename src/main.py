"""main.py — Pipeline orchestrator.

Executes BR_01 → BR_02 → BR_03 → BR_04 in sequence.
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

# BR_03
from src.br03.fetch_classified_for_cycle import fetch_classified_for_cycle
from src.br03.role_distribution_config import load_distribution_config
from src.br03.generate_digest import generate_digests
from src.br03.send_digest_email import send_digest_emails
from src.br03.send_digest_slack import send_digest_slack

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
    """Execute the full daily intelligence digest pipeline.

    Returns:
        0 — clean run (zero errors).
        1 — one or more errors occurred (pipeline continued where possible).
    """
    et = _ET_TZ
    pipeline_start = datetime.now(timezone.utc).astimezone(et)
    logger.info("=== Pipeline started: %s ===", pipeline_start.isoformat())

    errors: List[Dict[str, Any]] = []

    # ── Counters (all default to 0; updated as stages complete) ───────────
    feeds_attempted = 0
    feeds_errored = 0
    articles_fetched = 0
    articles_new = 0
    articles_classified = 0
    articles_classification_errors = 0
    digests_generated = 0
    emails_sent = 0
    emails_failed = 0
    slack_messages_sent = 0

    # ══════════════════════════════════════════════════════════════════════
    # BR_01 — Ingest RSS feeds
    # ══════════════════════════════════════════════════════════════════════
    logger.info("── BR_01: RSS Feed Ingestion ──")
    try:
        feed_list = load_feed_config()
        feeds_attempted = len(feed_list)
    except Exception as exc:
        msg = f"BR_01 config load failed — aborting pipeline: {exc}"
        logger.error(msg)
        errors.append(_err("BR_01_rss_feed_config.py", "SE", msg))
        _finish(pipeline_start, errors, feeds_attempted, feeds_errored,
                articles_fetched, articles_new, articles_classified,
                articles_classification_errors, digests_generated,
                emails_sent, emails_failed, slack_messages_sent)
        return 1

    try:
        fetched_articles = fetch_all_feeds(feed_list)
        articles_fetched = len(fetched_articles)
        # Feeds that contributed zero articles are treated as errored/silent
        active_sources = {a["source"] for a in fetched_articles}
        feeds_errored = feeds_attempted - len(active_sources)
    except Exception as exc:
        msg = f"BR_01 fetch_all_feeds unexpected failure: {exc}"
        logger.error(msg)
        errors.append(_err("BR_01_fetch_rss_feeds.py", "SE", msg))
        fetched_articles = []

    try:
        new_articles = deduplicate_articles(fetched_articles)
        articles_new = len(new_articles)
    except Exception as exc:
        msg = f"BR_01 deduplication failed: {exc}"
        logger.error(msg)
        errors.append(_err("BR_01_deduplicate_articles.py", "SE", msg))
        new_articles = []

    if new_articles:
        try:
            store_raw_articles(new_articles)
        except Exception as exc:
            msg = f"BR_01 store_raw_articles failed: {exc}"
            logger.error(msg)
            errors.append(_err("BR_01_store_raw_articles.py", "SE", msg))

    # ══════════════════════════════════════════════════════════════════════
    # BR_02 — Classify articles
    # ══════════════════════════════════════════════════════════════════════
    logger.info("── BR_02: Classification ──")
    classified_articles: list = []

    try:
        unprocessed = fetch_unprocessed_articles()
    except Exception as exc:
        msg = f"BR_02 fetch_unprocessed failed: {exc}"
        logger.error(msg)
        errors.append(_err("BR_02_fetch_unprocessed.py", "SE", msg))
        unprocessed = []

    if unprocessed:
        try:
            classified_articles = classify_articles(unprocessed)
            articles_classified = len(classified_articles)
            articles_classification_errors = len(unprocessed) - articles_classified
            if articles_classification_errors:
                errors.append(_err(
                    "BR_02_classify_article.py", "BE",
                    f"{articles_classification_errors} article(s) failed classification",
                    f"{articles_classification_errors}/{len(unprocessed)} articles",
                ))
        except RuntimeError as exc:
            # SE-01: auth failure aborts the classification batch
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
        try:
            stored_classified = store_classified_articles(classified_articles)
        except Exception as exc:
            msg = f"BR_02 store_classified failed: {exc}"
            logger.error(msg)
            errors.append(_err("BR_02_store_classified.py", "SE", msg))
            stored_classified = []

        if stored_classified:
            try:
                mark_articles_processed(stored_classified)
            except Exception as exc:
                msg = f"BR_02 mark_processed failed: {exc}"
                logger.error(msg)
                errors.append(_err("BR_02_mark_processed.py", "SE", msg))

    # ══════════════════════════════════════════════════════════════════════
    # BR_03 — Generate and deliver digests
    # ══════════════════════════════════════════════════════════════════════
    logger.info("── BR_03: Digest Generation & Delivery ──")

    try:
        articles_by_role = fetch_classified_for_cycle()
    except Exception as exc:
        msg = f"BR_03 fetch_classified_for_cycle failed: {exc}"
        logger.error(msg)
        errors.append(_err("BR_03_fetch_classified_for_cycle.py", "SE", msg))
        articles_by_role = {}

    try:
        stakeholders = load_distribution_config()
    except Exception as exc:
        msg = f"BR_03 load_distribution_config failed: {exc}"
        logger.error(msg)
        errors.append(_err("BR_03_role_distribution_config.py", "SE", msg))
        stakeholders = []

    digest_results: list = []
    if articles_by_role and stakeholders:
        try:
            digest_results = generate_digests(stakeholders, articles_by_role)
            digests_generated = len(digest_results)
        except Exception as exc:
            msg = f"BR_03 generate_digests failed: {exc}"
            logger.error(msg)
            errors.append(_err("BR_03_generate_digest.py", "SE", msg))

    if digest_results:
        try:
            email_results = send_digest_emails(digest_results)
            emails_sent = sum(1 for r in email_results if r.get("success"))
            emails_failed = sum(1 for r in email_results if not r.get("success"))
            for r in email_results:
                if not r.get("success"):
                    errors.append(_err(
                        "BR_03_send_digest_email.py", "SE",
                        r.get("error_if_any", "Email delivery failed"),
                        r.get("email", "N/A"),
                    ))
        except Exception as exc:
            msg = f"BR_03 send_digest_emails unexpected failure: {exc}"
            logger.error(msg)
            errors.append(_err("BR_03_send_digest_email.py", "SE", msg))

        try:
            slack_results = send_digest_slack(digest_results)
            slack_messages_sent = sum(1 for r in slack_results if r.get("success"))
            for r in slack_results:
                if not r.get("success"):
                    errors.append(_err(
                        "BR_03_send_digest_slack.py", "SE",
                        r.get("error_if_any", "Slack delivery failed"),
                        r.get("channel", "N/A"),
                    ))
        except Exception as exc:
            msg = f"BR_03 send_digest_slack unexpected failure: {exc}"
            logger.error(msg)
            errors.append(_err("BR_03_send_digest_slack.py", "SE", msg))

    # ══════════════════════════════════════════════════════════════════════
    # BR_04 — Summarise, log, notify
    # ══════════════════════════════════════════════════════════════════════
    _finish(
        pipeline_start, errors,
        feeds_attempted, feeds_errored,
        articles_fetched, articles_new,
        articles_classified, articles_classification_errors,
        digests_generated, emails_sent, emails_failed, slack_messages_sent,
    )

    exit_code = 0 if not errors else 1
    logger.info("=== Pipeline finished. Exit code: %d ===", exit_code)
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
    digests_generated: int,
    emails_sent: int,
    emails_failed: int,
    slack_messages_sent: int,
) -> None:
    """Run BR_04: compile summary, log errors, send completion email."""
    logger.info("── BR_04: Summary & Notification ──")
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
        digests_generated=digests_generated,
        emails_sent=emails_sent,
        emails_failed=emails_failed,
        slack_messages_sent=slack_messages_sent,
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
