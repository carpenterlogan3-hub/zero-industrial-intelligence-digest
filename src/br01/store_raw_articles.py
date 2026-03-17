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
