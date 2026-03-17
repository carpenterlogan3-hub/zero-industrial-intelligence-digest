"""BR_03_fetch_classified_for_cycle.py — V2

Query Google Sheets 'Classified Items' tab: column I (digest_date) = today (YYYY-MM-DD, ET).
Parse relevant_persons (comma-separated full names) into lists.
Build a dict keyed by person name, where each value is a list of article dicts assigned to that person.

Returns: Dict[str, List[Dict]] — e.g. {"Ted Kniesche": [{...}], "William Price": [{...}]}

V2 CHANGES:
- Groups by person name (not role key)
- relevant_persons column contains full names like "Ted Kniesche, Michael Brady"
- An article assigned to 3 people appears in all 3 person lists

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
    """Return today's classified articles grouped by person name.

    Reads 'Classified Items' tab filtered to today's digest_date (ET).
    Parses the comma-separated relevant_persons field and fans each article
    into every person's list.

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

    by_person: Dict[str, List[Dict]] = {}
    for row in rows:
        raw_persons = row.get("relevant_persons", "") or ""
        persons = [p.strip() for p in raw_persons.split(",") if p.strip()]
        for person in persons:
            by_person.setdefault(person, []).append(row)

    person_summary = {person: len(articles) for person, articles in by_person.items()}
    logger.info(
        "Fetched %d classified item(s) for %s. Recipients: %s",
        len(rows), today, person_summary,
    )
    return by_person
