"""One-time migration: rename column F header in 'Classified Items' tab
from 'relevant_roles' to 'relevant_persons'.

Run once from project root:
    python scripts/migrate_classified_header.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from src.reusable.sheets_data_layer import update_cell

TAB = "Classified Items"
OLD_HEADER = "relevant_roles"
NEW_HEADER = "relevant_persons"

if __name__ == "__main__":
    # update_cell resolves the column by its current header name, then writes to row 1
    result = update_cell(TAB, 1, OLD_HEADER, NEW_HEADER)
    if result:
        print(f"Success: column '{OLD_HEADER}' renamed to '{NEW_HEADER}' in '{TAB}' tab.")
    else:
        print("update_cell returned False — check logs.")
