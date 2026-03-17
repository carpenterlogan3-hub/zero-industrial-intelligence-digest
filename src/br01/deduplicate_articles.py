"""BR_01_deduplicate_articles.py

Read all existing URLs from Google Sheets 'Raw Feed Items' tab column D via
REUSABLE_sheets_data_layer. Load into Python set for O(1) lookup. Filter fetched
articles to new-only (URL not in set).

Exceptions:
    BE-01: All articles are duplicates (0 new) → log informational, skip store step.
"""
