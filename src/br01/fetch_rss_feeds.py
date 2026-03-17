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
