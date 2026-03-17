"""BR_01_fetch_rss_feeds.py

Iterate over validated feed list. For each feed, call feedparser.parse(url) with
15-second timeout. Filter entries to last 24 hours only. Extract: title, url,
summary (max 500 chars), pub_date, source, feed_category.

If a feed fails, log error and continue — do NOT abort.

Exceptions:
    SE-01: Feed unreachable (DNS, timeout, 4xx/5xx) → log, skip, continue.
    SE-02: HTTP 200 but invalid XML (HTML error page, CAPTCHA) → log, skip, continue.
    BE-01: Valid XML but zero entries in 24hr window → informational log, continue.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import feedparser
import requests

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 15
_SUMMARY_MAX_CHARS = 500
_WINDOW_HOURS = 24


def _fetch_feed_content(url: str) -> Optional[str]:
    """Fetch raw feed XML via requests with a 15s timeout. Returns None on failure."""
    try:
        response = requests.get(
            url,
            timeout=_TIMEOUT_SECONDS,
            headers={"User-Agent": "ZeroIndustrialDigest/1.0 (+https://zeroindustrial.energy)"},
        )
        response.raise_for_status()
        return response.text
    except requests.exceptions.Timeout:
        logger.error("SE-01: Timeout after %ds fetching feed: %s", _TIMEOUT_SECONDS, url)
    except requests.exceptions.ConnectionError as exc:
        logger.error("SE-01: Connection error fetching feed %s: %s", url, exc)
    except requests.exceptions.HTTPError as exc:
        logger.error("SE-01: HTTP %s fetching feed %s: %s", exc.response.status_code, url, exc)
    except requests.exceptions.RequestException as exc:
        logger.error("SE-01: Request failed for feed %s: %s", url, exc)
    return None


def _parse_pub_date(entry) -> Optional[datetime]:
    """Return a timezone-aware UTC datetime from feedparser entry, or None."""
    for attr in ("published_parsed", "updated_parsed", "created_parsed"):
        parsed = getattr(entry, attr, None)
        if parsed:
            try:
                return datetime(*parsed[:6], tzinfo=timezone.utc)
            except (ValueError, TypeError):
                continue
    return None


def _extract_url(entry) -> str:
    """Return the best URL for a feed entry."""
    return (
        getattr(entry, "link", None)
        or getattr(entry, "id", None)
        or ""
    )


def _extract_summary(entry) -> str:
    """Return cleaned summary text, capped at 500 chars."""
    raw = (
        getattr(entry, "summary", None)
        or getattr(entry, "description", None)
        or ""
    )
    # Strip HTML tags with a simple approach (feedparser usually handles this)
    import re
    clean = re.sub(r"<[^>]+>", " ", raw)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean[:_SUMMARY_MAX_CHARS]


def fetch_all_feeds(feed_list: List[Dict]) -> List[Dict]:
    """Fetch and filter articles from all feeds. Returns flat list of article dicts.

    Each article: {title, url, summary, pub_date, source, feed_category}
    Only articles published within the last 24 hours are included.
    Failed feeds are skipped — pipeline continues.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=_WINDOW_HOURS)
    articles: List[Dict] = []

    for feed in feed_list:
        url = feed["url"]
        name = feed["name"]
        category = feed["feed_category"]

        # SE-01: fetch with timeout
        content = _fetch_feed_content(url)
        if content is None:
            continue

        # SE-02: parse XML — feedparser won't raise but signals bozo on bad XML
        parsed = feedparser.parse(content)
        if parsed.get("bozo") and not parsed.get("entries"):
            exc = parsed.get("bozo_exception", "unknown parse error")
            logger.error("SE-02: Invalid feed XML from %s (%s): %s", name, url, exc)
            continue

        feed_articles_in_window = 0
        for entry in parsed.entries:
            pub_dt = _parse_pub_date(entry)

            # Skip entries with no parseable date only if we can't determine recency
            if pub_dt is None:
                logger.debug("No pub date on entry '%s' from %s — including with empty pub_date.", entry.get("title", "?"), name)
                pub_date_str = ""
            else:
                if pub_dt < cutoff:
                    continue
                pub_date_str = pub_dt.isoformat()

            entry_url = _extract_url(entry)
            if not entry_url:
                logger.debug("Skipping entry with no URL from feed %s.", name)
                continue

            articles.append({
                "title": (getattr(entry, "title", "") or "").strip(),
                "url": entry_url,
                "summary": _extract_summary(entry),
                "pub_date": pub_date_str,
                "source": name,
                "feed_category": category,
            })
            feed_articles_in_window += 1

        # BE-01: valid feed but nothing in window
        if feed_articles_in_window == 0:
            logger.info(
                "BE-01: Feed '%s' returned valid XML but 0 entries in last %dh.",
                name,
                _WINDOW_HOURS,
            )
        else:
            logger.info("Fetched %d article(s) from '%s'.", feed_articles_in_window, name)

    logger.info("Total articles fetched across all feeds: %d.", len(articles))
    return articles
