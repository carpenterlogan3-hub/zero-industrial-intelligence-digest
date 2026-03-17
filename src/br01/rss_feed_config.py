"""BR_01_rss_feed_config.py

Load config/feeds.yaml and validate its structure. Returns a flat list of feed
dicts: [{name, url, feed_category}, ...] with exactly 19 entries across 4 categories
(regulatory=5, ai_tech=5, energy_tes=5, business_finance=4).

Does NOT make HTTP calls — reachability is validated in fetch_rss_feeds.py.

Exceptions:
    SE-01: config/feeds.yaml missing or invalid YAML → abort pipeline, alert admin.
    BE-01: Valid YAML but missing category keys or zero feed URLs → abort pipeline, alert admin.
"""
