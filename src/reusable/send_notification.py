"""REUSABLE_send_notification.py

Multi-channel notification with prioritized fallback:
    1. Gmail API (primary) → refresh token if 401, retry 2x
    2. SMTP (first fallback) → config/smtp_config.yaml
    3. Slack (second fallback) → if slack_channel specified
    4. Local audit file (last resort) → logs/undelivered_{timestamp}.json

Parameters:
    to: str or List[str]
    cc: str or List[str] (optional)
    subject: str
    body: str
    body_is_html: bool
    attachments: List[str] (optional)
    escalate_on_failure: bool (default True)

Returns: {channel_used: str, success: bool, error_if_any: str or None}

Exceptions:
    SE-01: Gmail OAuth fail → token refresh, retry, fall to SMTP.
    SE-02: SMTP fail → fall to Slack.
    SE-03: All channels fail → write audit file. Raise if escalate_on_failure=True.
"""
