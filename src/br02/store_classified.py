"""BR_02_store_classified.py

Append enriched article to Google Sheets 'Classified Items' tab. Column mapping:
    A: title
    B: url
    C: source
    D: pub_date
    E: topic_category
    F: relevant_roles (comma-separated)
    G: importance
    H: one_line_summary
    I: digest_date (YYYY-MM-DD, ET timezone, today)

Exceptions:
    SE-01: Sheets write fail → retry 3x, log as unwritten, continue.
"""
