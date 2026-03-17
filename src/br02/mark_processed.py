"""BR_02_mark_processed.py

Update Google Sheets 'Raw Feed Items' tab: set column H from 'No' to 'Yes'
for each article successfully classified AND stored. Uses _row_number from
fetch_unprocessed output.

Exceptions:
    SE-01: Update fails → log error, continue. Article gets reclassified next run
           (idempotent, ~$0.001 cost per missed update).
"""
