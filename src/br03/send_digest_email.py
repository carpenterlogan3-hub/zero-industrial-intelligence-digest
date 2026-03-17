"""BR_03_send_digest_email.py

Send HTML digest to each stakeholder via REUSABLE_send_notification.
    Subject: "Zero Industrial Daily Intel Digest — {YYYY-MM-DD}"
    Body: HTML digest (BodyIsHTML=True)
    EscalateOnFailure: True

Exceptions:
    SE-01: Gmail auth 401 → token refresh, retry, SMTP fallback.
    SE-02: Gmail quota 429/503 → retry, SMTP fallback, Slack fallback.
    BE-01: Invalid recipient address → log, alert admin, continue with others.
"""
