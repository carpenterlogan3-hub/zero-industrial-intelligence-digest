"""BR_03_fetch_classified_for_cycle.py

Query Google Sheets 'Classified Items' tab: column I (digest_date) = today (YYYY-MM-DD, ET).
Parse relevant_roles (comma-separated) into lists. Build dict keyed by role.

Returns: Dict[str, List[Dict]] — e.g. {"CEO": [{...}], "VP_Finance": [{...}]}

Exceptions:
    BE-01: Zero items for today → notify admin, skip digest generation.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List
from zoneinfo import ZoneInfo

from src.reusable.sheets_data_layer import read_rows

logger = logging.getLogger(__name__)

_TAB_NAME = "Classified Items"
_DATE_COLUMN = "digest_date"
_ET_TZ = ZoneInfo("America/New_York")


def _today_et() -> str:
    return datetime.now(timezone.utc).astimezone(_ET_TZ).strftime("%Y-%m-%d")


def fetch_classified_for_cycle() -> Dict[str, List[Dict]]:
    """Return today's classified articles grouped by role.

    Reads 'Classified Items' tab filtered to today's digest_date (ET).
    Parses the comma-separated relevant_roles field and fans each article
    out into every role it belongs to.

    Returns empty dict (with BE-01 log) if no articles found for today.
    """
    today = _today_et()
    rows = read_rows(_TAB_NAME, filter_column=_DATE_COLUMN, filter_value=today)

    if not rows:
        logger.info(
            "BE-01: Zero classified items for digest_date=%s. Skipping digest generation.",
            today,
        )
        return {}

    by_role: Dict[str, List[Dict]] = {}
    for row in rows:
        raw_roles = row.get("relevant_roles", "") or ""
        roles = [r.strip() for r in raw_roles.split(",") if r.strip()]
        for role in roles:
            by_role.setdefault(role, []).append(row)

    role_summary = {role: len(articles) for role, articles in by_role.items()}
    logger.info(
        "Fetched %d classified item(s) for %s. Roles: %s",
        len(rows), today, role_summary,
    )
    return by_role
