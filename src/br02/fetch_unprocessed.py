"""BR_02_fetch_unprocessed.py

Query Google Sheets 'Raw Feed Items' tab: all rows where column H = 'No'.
Returns list of dicts with all 8 columns + _row_number for later update.

Exceptions:
    SE-01: Sheets 401/403/404 → retry 3x, abort + alert.
    BE-01: Zero unprocessed rows → log informational, skip to BR_03.
"""
