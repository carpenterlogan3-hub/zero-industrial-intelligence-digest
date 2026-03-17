"""BR_04_log_errors.py

Write all error records to Google Sheets 'Errors' tab. Column mapping:
    A: timestamp (ISO 8601, ET)
    B: pipeline_run_date (YYYY-MM-DD)
    C: module_name (e.g. 'BR_01_fetch_rss_feeds.py')
    D: error_type ('SE' or 'BE')
    E: error_message (max 500 chars)
    F: affected_item (article title/URL or 'N/A')

If zero errors, skip entirely. Do not write empty rows.

Exceptions:
    SE-01: Sheets write fail → fallback to logs/errors_{date}.json.
"""
