"""REUSABLE_sheets_data_layer.py

All Google Sheets operations via gspread. Authenticates with service account JSON
at path GOOGLE_SERVICE_ACCOUNT_PATH env var. Spreadsheet ID from SPREADSHEET_ID env var.

Operations:
    read_rows(tab_name, filter_column=None, filter_value=None) → List[Dict] with _row_number
    append_row(tab_name, row_data: Dict) → int (row number)
    update_cell(tab_name, row_number: int, column: str, value: str) → bool
    search_column(tab_name, column: str) → List[str]

All operations retry 3x with exponential backoff (2s/4s/8s) for 429/503.

Exceptions:
    SE-01: Service account JSON missing/invalid → raise immediately.
    SE-02: Sheets API 429 (60 req/min limit) → backoff 3x, then raise.
"""

import os
import time
import logging
from typing import Dict, List, Optional

import gspread
from gspread.exceptions import APIError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_RETRY_DELAYS = [2, 4, 8]
_RETRYABLE_CODES = {429, 503}


def _get_client() -> gspread.Client:
    sa_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_PATH")
    if not sa_path or not os.path.isfile(sa_path):
        raise FileNotFoundError(
            f"SE-01: Service account JSON missing or invalid at path: {sa_path!r}"
        )
    return gspread.service_account(filename=sa_path)


def _get_spreadsheet_id() -> str:
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        raise ValueError("SE-01: SPREADSHEET_ID environment variable is not set.")
    return spreadsheet_id


def _with_retry(fn):
    """Execute fn(), retrying on 429/503 with exponential backoff (2s, 4s, 8s)."""
    last_exc = None
    for attempt, delay in enumerate([0] + _RETRY_DELAYS):
        if delay:
            logger.warning("SE-02: Retryable error, backing off %ds (attempt %d/3).", delay, attempt)
            time.sleep(delay)
        try:
            return fn()
        except APIError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in _RETRYABLE_CODES:
                last_exc = exc
            else:
                raise
    raise last_exc


def _get_worksheet(tab_name: str) -> gspread.Worksheet:
    client = _get_client()
    spreadsheet_id = _get_spreadsheet_id()
    spreadsheet = client.open_by_key(spreadsheet_id)
    return spreadsheet.worksheet(tab_name)


def read_rows(
    tab_name: str,
    filter_column: Optional[str] = None,
    filter_value: Optional[str] = None,
) -> List[Dict]:
    """Return all rows from tab_name as a list of dicts.

    Each dict includes a '_row_number' key (1-based, accounting for header row).
    If filter_column and filter_value are provided, only matching rows are returned.
    """
    def _read():
        ws = _get_worksheet(tab_name)
        records = ws.get_all_records()
        result = []
        for i, record in enumerate(records):
            row = dict(record)
            row["_row_number"] = i + 2  # +1 for 0-index, +1 for header row
            if filter_column is not None and filter_value is not None:
                if str(row.get(filter_column, "")) != str(filter_value):
                    continue
            result.append(row)
        return result

    return _with_retry(_read)


def append_row(tab_name: str, row_data: Dict) -> int:
    """Append a row to tab_name using the sheet's header order.

    Returns the 1-based row number of the appended row.
    """
    def _append():
        ws = _get_worksheet(tab_name)
        headers = ws.row_values(1)
        row_values = [str(row_data.get(h, "")) for h in headers]
        ws.append_row(row_values, value_input_option="USER_ENTERED")
        # The appended row is at the current last row
        return ws.row_count

    return _with_retry(_append)


def update_cell(tab_name: str, row_number: int, column: str, value: str) -> bool:
    """Update a single cell identified by row_number (1-based) and column header name.

    Returns True on success.
    """
    def _update():
        ws = _get_worksheet(tab_name)
        headers = ws.row_values(1)
        if column not in headers:
            raise ValueError(f"Column '{column}' not found in tab '{tab_name}'. Headers: {headers}")
        col_index = headers.index(column) + 1  # gspread uses 1-based column index
        ws.update_cell(row_number, col_index, value)
        return True

    return _with_retry(_update)


def search_column(tab_name: str, column: str) -> List[str]:
    """Return all values in the given column (excluding the header).

    Returns a list of string values, empty strings included.
    """
    def _search():
        ws = _get_worksheet(tab_name)
        headers = ws.row_values(1)
        if column not in headers:
            raise ValueError(f"Column '{column}' not found in tab '{tab_name}'. Headers: {headers}")
        col_index = headers.index(column) + 1  # 1-based
        all_values = ws.col_values(col_index)
        return all_values[1:]  # strip header

    return _with_retry(_search)
