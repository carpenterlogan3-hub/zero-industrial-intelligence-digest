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
