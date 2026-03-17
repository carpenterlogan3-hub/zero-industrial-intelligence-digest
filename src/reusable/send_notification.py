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

import base64
import json
import logging
import mimetypes
import os
import smtplib
from datetime import datetime, timezone
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional, Union

import yaml
from dotenv import load_dotenv
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "email_config.yaml"
_LOGS_DIR = Path(__file__).parent.parent.parent / "logs"
_GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalise_recipients(value: Optional[Union[str, List[str]]]) -> List[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def _load_email_config() -> Dict:
    with open(_CONFIG_PATH, "r") as fh:
        return yaml.safe_load(fh)


def _build_mime(
    sender: str,
    to: List[str],
    cc: List[str],
    subject: str,
    body: str,
    body_is_html: bool,
    attachments: List[str],
    reply_to: Optional[str] = None,
) -> MIMEMultipart:
    msg = MIMEMultipart("mixed")
    msg["From"] = sender
    msg["To"] = ", ".join(to)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg["Subject"] = subject
    if reply_to:
        msg["Reply-To"] = reply_to

    content_type = "html" if body_is_html else "plain"
    msg.attach(MIMEText(body, content_type, "utf-8"))

    for path_str in attachments:
        path = Path(path_str)
        mime_type, _ = mimetypes.guess_type(path_str)
        main_type, sub_type = (mime_type or "application/octet-stream").split("/", 1)
        with open(path, "rb") as fh:
            part = MIMEBase(main_type, sub_type)
            part.set_payload(fh.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename=path.name)
        msg.attach(part)

    return msg


# ---------------------------------------------------------------------------
# Channel 1: Gmail API
# ---------------------------------------------------------------------------

def _get_gmail_credentials(cfg: Dict) -> Credentials:
    token_path = Path(cfg["token_path"])
    creds_path = Path(cfg["oauth_credentials_path"])
    creds = None

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), _GMAIL_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), _GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w") as fh:
            fh.write(creds.to_json())

    return creds


def _send_via_gmail(
    cfg: Dict,
    to: List[str],
    cc: List[str],
    subject: str,
    body: str,
    body_is_html: bool,
    attachments: List[str],
) -> None:
    sender = cfg["sender_address"]
    reply_to = cfg.get("reply_to")
    mime = _build_mime(sender, to, cc, subject, body, body_is_html, attachments, reply_to)
    raw = base64.urlsafe_b64encode(mime.as_bytes()).decode()

    last_exc = None
    for attempt in range(3):  # initial try + 2 retries
        try:
            creds = _get_gmail_credentials(cfg)
            service = build("gmail", "v1", credentials=creds, cache_discovery=False)
            service.users().messages().send(userId="me", body={"raw": raw}).execute()
            return
        except (HttpError, RefreshError) as exc:
            status = exc.resp.status if hasattr(exc, "resp") else None
            if isinstance(exc, RefreshError) or status == 401:
                logger.warning("SE-01: Gmail auth error on attempt %d, refreshing token.", attempt + 1)
                token_path = Path(cfg["token_path"])
                if token_path.exists():
                    token_path.unlink()
                last_exc = exc
            else:
                raise
    raise RuntimeError(f"SE-01: Gmail failed after token refresh retries: {last_exc}") from last_exc


# ---------------------------------------------------------------------------
# Channel 2: SMTP
# ---------------------------------------------------------------------------

def _send_via_smtp(
    cfg: Dict,
    to: List[str],
    cc: List[str],
    subject: str,
    body: str,
    body_is_html: bool,
    attachments: List[str],
) -> None:
    smtp_cfg = cfg["smtp_fallback"]
    sender = cfg["sender_address"]
    reply_to = cfg.get("reply_to")
    mime = _build_mime(sender, to, cc, subject, body, body_is_html, attachments, reply_to)

    username = os.environ.get("SMTP_USERNAME", sender)
    password = os.environ.get("SMTP_PASSWORD")
    if not password:
        raise RuntimeError("SE-02: SMTP_PASSWORD env var not set.")

    host = smtp_cfg["host"]
    port = smtp_cfg["port"]
    use_tls = smtp_cfg.get("use_tls", True)

    recipients = to + cc
    with smtplib.SMTP(host, port) as server:
        if use_tls:
            server.starttls()
        server.login(username, password)
        server.sendmail(sender, recipients, mime.as_string())


# ---------------------------------------------------------------------------
# Channel 3: Slack
# ---------------------------------------------------------------------------

def _send_via_slack(
    slack_channel: str,
    subject: str,
    body: str,
    body_is_html: bool,
) -> None:
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        raise RuntimeError("SLACK_BOT_TOKEN env var not set.")

    client = WebClient(token=token)

    # Strip HTML tags for Slack plain-text fallback
    if body_is_html:
        import re
        plain_body = re.sub(r"<[^>]+>", "", body).strip()
    else:
        plain_body = body

    text = f"*{subject}*\n{plain_body}"
    try:
        client.chat_postMessage(channel=slack_channel, text=text)
    except SlackApiError as exc:
        raise RuntimeError(f"Slack API error: {exc.response['error']}") from exc


# ---------------------------------------------------------------------------
# Channel 4: Local audit file
# ---------------------------------------------------------------------------

def _write_audit_file(
    to: List[str],
    cc: List[str],
    subject: str,
    body: str,
    errors: Dict[str, str],
) -> str:
    _LOGS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    audit_path = _LOGS_DIR / f"undelivered_{timestamp}.json"
    payload = {
        "timestamp": timestamp,
        "to": to,
        "cc": cc,
        "subject": subject,
        "body_preview": body[:500],
        "delivery_errors": errors,
    }
    with open(audit_path, "w") as fh:
        json.dump(payload, fh, indent=2)
    logger.error("SE-03: All channels failed. Audit written to %s", audit_path)
    return str(audit_path)


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def send_notification(
    to: Union[str, List[str]],
    subject: str,
    body: str,
    body_is_html: bool = False,
    cc: Optional[Union[str, List[str]]] = None,
    attachments: Optional[List[str]] = None,
    slack_channel: Optional[str] = None,
    escalate_on_failure: bool = True,
) -> Dict:
    """Send a notification via the 4-channel fallback chain.

    Returns:
        {
            "channel_used": str,   # "gmail" | "smtp" | "slack" | "audit_file"
            "success": bool,
            "error_if_any": str | None,
        }
    """
    to_list = _normalise_recipients(to)
    cc_list = _normalise_recipients(cc)
    attachments = attachments or []
    errors: Dict[str, str] = {}

    try:
        cfg = _load_email_config()
    except Exception as exc:
        cfg = None
        logger.error("Failed to load email_config.yaml: %s", exc)
        errors["config"] = str(exc)

    # --- Channel 1: Gmail API ---
    if cfg is not None:
        try:
            _send_via_gmail(cfg, to_list, cc_list, subject, body, body_is_html, attachments)
            logger.info("Notification sent via Gmail to %s", to_list)
            return {"channel_used": "gmail", "success": True, "error_if_any": None}
        except Exception as exc:
            logger.warning("SE-01: Gmail failed: %s. Falling back to SMTP.", exc)
            errors["gmail"] = str(exc)

    # --- Channel 2: SMTP ---
    if cfg is not None:
        try:
            _send_via_smtp(cfg, to_list, cc_list, subject, body, body_is_html, attachments)
            logger.info("Notification sent via SMTP to %s", to_list)
            return {"channel_used": "smtp", "success": True, "error_if_any": None}
        except Exception as exc:
            logger.warning("SE-02: SMTP failed: %s. Falling back to Slack.", exc)
            errors["smtp"] = str(exc)

    # --- Channel 3: Slack ---
    if slack_channel:
        try:
            _send_via_slack(slack_channel, subject, body, body_is_html)
            logger.info("Notification sent via Slack to %s", slack_channel)
            return {"channel_used": "slack", "success": True, "error_if_any": None}
        except Exception as exc:
            logger.warning("Slack failed: %s. Falling back to audit file.", exc)
            errors["slack"] = str(exc)
    else:
        errors["slack"] = "No slack_channel specified."

    # --- Channel 4: Local audit file ---
    audit_path = _write_audit_file(to_list, cc_list, subject, body, errors)
    error_summary = "; ".join(f"{ch}: {msg}" for ch, msg in errors.items())
    result = {
        "channel_used": "audit_file",
        "success": False,
        "error_if_any": f"SE-03: All channels failed. Audit: {audit_path}. Errors — {error_summary}",
    }

    if escalate_on_failure:
        raise RuntimeError(result["error_if_any"])

    return result
