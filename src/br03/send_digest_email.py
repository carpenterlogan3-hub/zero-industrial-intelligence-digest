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

import logging
import re
from datetime import datetime, timezone
from typing import Dict, List
from zoneinfo import ZoneInfo

from src.reusable.send_notification import send_notification

logger = logging.getLogger(__name__)

_ET_TZ = ZoneInfo("America/New_York")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _today_label() -> str:
    return datetime.now(timezone.utc).astimezone(_ET_TZ).strftime("%Y-%m-%d")


def _is_valid_email(address: str) -> bool:
    return bool(_EMAIL_RE.match(address.strip()))


def send_digest_emails(digest_results: List[Dict]) -> List[Dict]:
    """Send an HTML digest email to each stakeholder.

    digest_results is the list returned by generate_digests:
        [{stakeholder: {...}, digest_html: str, article_count: int}, ...]

    Returns a list of delivery result dicts:
        {name, email, channel_used, success, error_if_any}
    """
    date_label = _today_label()
    subject = f"Zero Industrial Daily Intel Digest \u2014 {date_label}"
    delivery_results = []

    for item in digest_results:
        stakeholder = item["stakeholder"]
        digest_html = item["digest_html"]
        name = stakeholder.get("name", "Unknown")
        email = stakeholder.get("email", "")
        slack_channel = stakeholder.get("slack_channel") or None

        # BE-01: invalid email address
        if not _is_valid_email(email):
            logger.error(
                "BE-01: Invalid recipient address '%s' for stakeholder '%s' — skipping.",
                email, name,
            )
            delivery_results.append({
                "name": name,
                "email": email,
                "channel_used": None,
                "success": False,
                "error_if_any": f"BE-01: Invalid email address: '{email}'",
            })
            continue

        try:
            result = send_notification(
                to=email,
                subject=subject,
                body=digest_html,
                body_is_html=True,
                slack_channel=slack_channel,
                escalate_on_failure=True,
            )
            delivery_results.append({
                "name": name,
                "email": email,
                **result,
            })
            logger.info(
                "Digest sent to '%s' <%s> via %s.",
                name, email, result.get("channel_used"),
            )
        except RuntimeError as exc:
            # escalate_on_failure=True means send_notification raises after all channels fail
            logger.error(
                "All delivery channels failed for '%s' <%s>: %s",
                name, email, exc,
            )
            delivery_results.append({
                "name": name,
                "email": email,
                "channel_used": "audit_file",
                "success": False,
                "error_if_any": str(exc),
            })

    successful = sum(1 for r in delivery_results if r.get("success"))
    logger.info(
        "Email delivery complete: %d/%d succeeded.", successful, len(delivery_results)
    )
    return delivery_results
