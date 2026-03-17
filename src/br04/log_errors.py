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

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from src.reusable.sheets_data_layer import append_row

logger = logging.getLogger(__name__)

_TAB_NAME = "Errors"
_ET_TZ = ZoneInfo("America/New_York")
_LOGS_DIR = Path(__file__).parent.parent.parent / "logs"


def _now_et() -> datetime:
    return datetime.now(timezone.utc).astimezone(_ET_TZ)


def log_errors(errors: List[Dict[str, Any]], pipeline_run_date: str = "") -> int:
    """Write error records to 'Errors' Sheets tab. Returns count of records written.

    Each error record dict should contain:
        module_name, error_type, error_message, affected_item (optional)

    If Sheets write fails, falls back to logs/errors_{date}.json.
    Skips entirely if errors list is empty.
    """
    if not errors:
        logger.info("No errors to log — skipping Errors tab write.")
        return 0

    now = _now_et()
    timestamp_iso = now.isoformat()
    run_date = pipeline_run_date or now.strftime("%Y-%m-%d")
    written = 0
    sheets_failed = False

    for error in errors:
        row_data = {
            "timestamp": timestamp_iso,
            "pipeline_run_date": run_date,
            "module_name": str(error.get("module_name", "unknown")),
            "error_type": str(error.get("error_type", "SE")),
            "error_message": str(error.get("error_message", ""))[:500],
            "affected_item": str(error.get("affected_item", "N/A")),
        }
        if not sheets_failed:
            try:
                append_row(_TAB_NAME, row_data)
                written += 1
            except Exception as exc:
                logger.error(
                    "SE-01: Failed to write to Errors Sheets tab: %s — switching to JSON fallback.",
                    exc,
                )
                sheets_failed = True

    if sheets_failed:
        written = _write_json_fallback(errors, run_date, timestamp_iso)

    logger.info("Logged %d/%d error(s).", written, len(errors))
    return written


def _write_json_fallback(
    errors: List[Dict[str, Any]],
    run_date: str,
    timestamp_iso: str,
) -> int:
    """Write all errors to a local JSON file. Returns count written."""
    _LOGS_DIR.mkdir(parents=True, exist_ok=True)
    date_slug = run_date.replace("-", "")
    path = _LOGS_DIR / f"errors_{date_slug}.json"

    payload = {
        "pipeline_run_date": run_date,
        "written_at": timestamp_iso,
        "errors": errors,
    }
    try:
        with open(path, "w") as fh:
            json.dump(payload, fh, indent=2, default=str)
        logger.info("SE-01: Error fallback written to %s (%d records).", path, len(errors))
        return len(errors)
    except Exception as exc:
        logger.error("SE-01: JSON fallback also failed: %s", exc)
        # Last resort: dump to stderr
        for err in errors:
            logger.error("UNLOGGED ERROR: %s", err)
        return 0
