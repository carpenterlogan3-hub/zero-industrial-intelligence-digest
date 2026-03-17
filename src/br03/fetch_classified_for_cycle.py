"""BR_03_fetch_classified_for_cycle.py

Query Google Sheets 'Classified Items' tab: column I (digest_date) = today (YYYY-MM-DD, ET).
Parse relevant_roles (comma-separated) into lists. Build dict keyed by role.

Returns: Dict[str, List[Dict]] — e.g. {"CEO": [{...}], "VP_Finance": [{...}]}

Exceptions:
    BE-01: Zero items for today → notify admin, skip digest generation.
"""
