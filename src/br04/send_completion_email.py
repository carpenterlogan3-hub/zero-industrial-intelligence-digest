"""BR_04_send_completion_email.py

Send HTML completion summary to admin (Logan Carpenter) via REUSABLE_send_notification.
    Subject: "Pipeline Complete — {YYYY-MM-DD} — {total_errors} errors"
    Body: HTML table of all 15 metrics + error highlights + link to Errors tab.
    EscalateOnFailure: False (failure here does not crash pipeline).

Exceptions:
    SE-01: All channels fail → write to stdout + logs/completion_{date}.json.
"""
