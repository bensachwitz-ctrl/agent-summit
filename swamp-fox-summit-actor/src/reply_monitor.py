"""Reply monitor — polls Gmail IMAP for replies, captures the message body,
marks the lead as replied, and triggers a notification alert to Ben with
full lead intelligence attached.

Reply detection logic:
  1. Search inbox for messages from any contacted lead's email address
  2. For each match, parse the body (text/plain preferred, HTML fallback)
  3. Update lead state: replied=True, engagement_status=replied, reply_received_at=now
  4. Send alert email to Ben with intel brief + reply excerpt
"""
from __future__ import annotations

import asyncio
import email
import html
import imaplib
import re
from datetime import datetime, timezone
from email.header import decode_header
from email.message import Message
from typing import Any

from apify import Actor

from .reply_alerts import send_reply_alert

GMAIL_IMAP_HOST = "imap.gmail.com"
GMAIL_IMAP_PORT = 993


def _decode(value: str | None) -> str:
    if not value:
        return ""
    parts = decode_header(value)
    out = ""
    for content, encoding in parts:
        if isinstance(content, bytes):
            try:
                out += content.decode(encoding or "utf-8", errors="replace")
            except (LookupError, UnicodeDecodeError):
                out += content.decode("utf-8", errors="replace")
        else:
            out += content
    return out


def _extract_body(msg: Message) -> str:
    """Pull plain text body, falling back to stripped HTML."""
    text_body = ""
    html_body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = str(part.get("Content-Disposition") or "")
            if "attachment" in disp:
                continue
            payload = part.get_payload(decode=True)
            if payload is None:
                continue
            charset = part.get_content_charset() or "utf-8"
            try:
                decoded = payload.decode(charset, errors="replace")
            except (LookupError, UnicodeDecodeError):
                decoded = payload.decode("utf-8", errors="replace")
            if ctype == "text/plain" and not text_body:
                text_body = decoded
            elif ctype == "text/html" and not html_body:
                html_body = decoded
    else:
        payload = msg.get_payload(decode=True)
        if payload is not None:
            charset = msg.get_content_charset() or "utf-8"
            try:
                text_body = payload.decode(charset, errors="replace")
            except (LookupError, UnicodeDecodeError):
                text_body = payload.decode("utf-8", errors="replace")

    if text_body:
        return _strip_quoted(text_body)
    if html_body:
        # Strip tags
        stripped = re.sub(r"<[^>]+>", " ", html_body)
        stripped = html.unescape(stripped)
        stripped = re.sub(r"\s+", " ", stripped)
        return _strip_quoted(stripped.strip())
    return ""


def _strip_quoted(text: str) -> str:
    """Remove the quoted prior message from a reply, keeping just the new content."""
    # Common reply markers
    markers = [
        r"^On .* wrote:.*$",
        r"^From: .*$",
        r"^-----Original Message-----",
        r"^>{1,}\s",
    ]
    lines = text.splitlines()
    cutoff = len(lines)
    for i, line in enumerate(lines):
        if any(re.match(p, line.strip(), re.IGNORECASE) for p in markers):
            cutoff = i
            break
    return "\n".join(lines[:cutoff]).strip()


def _check_replies_sync(
    sender_email: str,
    app_password: str,
    known_emails: set[str],
    since: str = "1-Jul-2026",
) -> list[dict[str, Any]]:
    """Returns list of {email, subject, body, received_at} for new replies."""
    replies: list[dict[str, Any]] = []
    try:
        mail = imaplib.IMAP4_SSL(GMAIL_IMAP_HOST, GMAIL_IMAP_PORT)
        mail.login(sender_email, app_password)
        mail.select("INBOX")

        status, data = mail.search(None, f'(SINCE "{since}")')
        if status != "OK":
            return replies

        ids = data[0].split()
        seen_addrs: set[str] = set()
        # Newest first — limit to last 1000 messages
        for msg_id in reversed(ids[-1000:]):
            status, msg_data = mail.fetch(msg_id, "(RFC822)")
            if status != "OK":
                continue
            for part in msg_data:
                if not isinstance(part, tuple):
                    continue
                msg = email.message_from_bytes(part[1])
                from_header = _decode(msg.get("From", ""))
                m = re.search(r"[\w._%+-]+@[\w.-]+\.\w+", from_header)
                if not m:
                    continue
                addr = m.group(0).lower()
                if addr not in known_emails or addr in seen_addrs:
                    continue
                seen_addrs.add(addr)

                subject = _decode(msg.get("Subject", ""))
                body = _extract_body(msg)
                received = msg.get("Date") or datetime.now(timezone.utc).isoformat()

                replies.append({
                    "email": addr,
                    "subject": subject,
                    "body": body,
                    "received_at": received,
                })

        mail.close()
        mail.logout()
    except imaplib.IMAP4.error as e:
        Actor.log.warning(f"IMAP error during reply check: {e}")
    except Exception as e:
        Actor.log.warning(f"Unexpected error during reply check: {e}")
    return replies


async def check_for_replies(cfg: dict[str, Any], storage) -> int:
    """Pull leads, check inbox, mark replies, trigger alerts. Returns count of new replies."""
    if not cfg.get("enableReplyMonitor", True):
        return 0

    sender = cfg.get("senderEmail")
    app_pw = cfg.get("gmailAppPassword")
    if not sender or not app_pw:
        Actor.log.info("Reply monitor disabled (missing credentials).")
        return 0

    leads = await storage.fetch_leads(filters={})
    email_to_lead = {
        (l.get("email") or "").lower(): l
        for l in leads
        if l.get("email") and not l.get("replied") and l.get("first_email_sent_at")
    }
    if not email_to_lead:
        return 0

    Actor.log.info(f"Checking inbox for replies from {len(email_to_lead)} contacted leads")
    replies = await asyncio.to_thread(
        _check_replies_sync, sender, app_pw, set(email_to_lead.keys())
    )

    new_count = 0
    for reply in replies:
        addr = reply["email"]
        lead = email_to_lead.get(addr)
        if not lead:
            continue

        await storage.update_lead(lead["lead_id"], {
            "replied": True,
            "reply_received_at": datetime.now(timezone.utc).isoformat(),
            "reply_subject": reply["subject"][:200],
            "reply_body_excerpt": reply["body"][:1000],
            "engagement_status": "replied",
        })

        # Refetch with updated state for the alert
        updated = await storage.kv_store.get_value(f"LEAD_{lead['lead_id']}")
        await send_reply_alert(updated or lead, cfg, reply_excerpt=reply["body"])

        new_count += 1
        Actor.log.info(f"Reply detected from {addr} ({lead.get('company_name')}) — alert sent")

    return new_count
