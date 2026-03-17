"""BR_03_send_digest_slack.py

For stakeholders with non-null slack_channel (currently Logan only → #digest-ai),
post plain-text version of digest to Slack. Runs AFTER email delivery.

Exceptions:
    SE-01: Slack auth error → log, skip all Slack, rely on email.
    SE-02: Channel not found → log, alert admin, skip this post.
"""

import logging
import os
import re
from datetime import datetime, timezone
from typing import Dict, List
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_ET_TZ = ZoneInfo("America/New_York")
_AUTH_ERRORS = {"invalid_auth", "not_authed", "token_revoked", "account_inactive"}
_CHANNEL_ERRORS = {"channel_not_found", "is_archived", "not_in_channel"}


def _today_label() -> str:
    return datetime.now(timezone.utc).astimezone(_ET_TZ).strftime("%Y-%m-%d")


def _html_to_plain(html: str) -> str:
    """Convert HTML digest to readable Slack plain text."""
    text = html

    # Convert headings to bold-ish plain text with spacing
    text = re.sub(r"<h[1-3][^>]*>(.*?)</h[1-3]>", r"\n*\1*\n", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<h[4-6][^>]*>(.*?)</h[4-6]>", r"\n\1\n", text, flags=re.IGNORECASE | re.DOTALL)

    # Convert anchor tags to "text (url)" format
    text = re.sub(
        r'<a\s+[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        r"\2 (\1)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # Convert list items to bullet points
    text = re.sub(r"<li[^>]*>(.*?)</li>", r"\n• \1", text, flags=re.IGNORECASE | re.DOTALL)

    # Convert <br> and <p> to newlines
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)

    # Strip all remaining HTML tags
    text = re.sub(r"<[^>]+>", "", text)

    # Decode common HTML entities
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&nbsp;", " ").replace("&#8212;", "—").replace("&mdash;", "—")
    text = text.replace("&ndash;", "–").replace("&quot;", '"').replace("&#39;", "'")

    # Collapse excessive whitespace while preserving paragraph breaks
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)

    return text.strip()


def send_digest_slack(digest_results: List[Dict]) -> List[Dict]:
    """Post plain-text digest to each stakeholder's Slack channel.

    Only processes stakeholders with a non-null slack_channel field.
    Runs after email delivery — Slack is supplementary, not a fallback here.

    Returns list of delivery result dicts: {name, channel, success, error_if_any}
    """
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        logger.warning("SE-01: SLACK_BOT_TOKEN not set — skipping all Slack notifications.")
        return []

    client = WebClient(token=token)
    date_label = _today_label()
    results = []
    auth_failed = False

    for item in digest_results:
        stakeholder = item["stakeholder"]
        name = stakeholder.get("name", "Unknown")
        slack_channel = stakeholder.get("slack_channel") or None

        if not slack_channel:
            continue

        if auth_failed:
            logger.warning("SE-01: Skipping Slack post for '%s' — auth already failed.", name)
            results.append({
                "name": name,
                "channel": slack_channel,
                "success": False,
                "error_if_any": "SE-01: Skipped due to prior auth failure.",
            })
            continue

        plain_text = _html_to_plain(item["digest_html"])
        header = f"*Zero Industrial Daily Intel Digest — {date_label}*\n_{name}_\n\n"
        message = header + plain_text

        try:
            client.chat_postMessage(channel=slack_channel, text=message)
            logger.info("Slack digest posted for '%s' → %s.", name, slack_channel)
            results.append({
                "name": name,
                "channel": slack_channel,
                "success": True,
                "error_if_any": None,
            })
        except SlackApiError as exc:
            error_code = exc.response.get("error", "") if exc.response else ""

            if error_code in _AUTH_ERRORS:
                logger.error(
                    "SE-01: Slack auth error '%s' for '%s' — skipping all remaining Slack posts.",
                    error_code, name,
                )
                auth_failed = True
                results.append({
                    "name": name,
                    "channel": slack_channel,
                    "success": False,
                    "error_if_any": f"SE-01: Auth error: {error_code}",
                })
            elif error_code in _CHANNEL_ERRORS:
                logger.error(
                    "SE-02: Slack channel '%s' not found/accessible for '%s' (%s) — skipping.",
                    slack_channel, name, error_code,
                )
                results.append({
                    "name": name,
                    "channel": slack_channel,
                    "success": False,
                    "error_if_any": f"SE-02: Channel error: {error_code}",
                })
            else:
                logger.error(
                    "Unexpected Slack error posting to '%s' for '%s': %s",
                    slack_channel, name, exc,
                )
                results.append({
                    "name": name,
                    "channel": slack_channel,
                    "success": False,
                    "error_if_any": str(exc),
                })

    successful = sum(1 for r in results if r.get("success"))
    if results:
        logger.info("Slack delivery complete: %d/%d succeeded.", successful, len(results))
    return results
