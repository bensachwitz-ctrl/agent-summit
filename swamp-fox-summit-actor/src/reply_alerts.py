"""Reply alert system — when a lead replies, send a notification email to Ben
with the full lead intelligence brief and the original reply attached.

The notification is formatted for fast scanning so Ben can decide whether
to respond personally and what angle to take.
"""
from __future__ import annotations

import asyncio
import smtplib
import ssl
from email.message import EmailMessage
from typing import Any

from apify import Actor

from .lead_intel import build_intel_brief

GMAIL_SMTP_HOST = "smtp.gmail.com"
GMAIL_SMTP_PORT = 465


def _build_alert_body(lead: dict[str, Any], reply_excerpt: str = "") -> str:
    """Construct the notification body Ben sees."""
    brief = build_intel_brief(lead)

    parts = [
        "🔔 LEAD REPLY DETECTED",
        "",
        f"Lead: {lead.get('company_name', 'Unknown')}",
        f"Contact: {lead.get('contact_name') or 'unknown'} ({lead.get('contact_title') or 'role unknown'})",
        f"Email: {lead.get('email')}",
        f"Phone: {lead.get('phone') or 'not on file'}",
        f"Producer assigned: {lead.get('producer_assigned', 'unassigned')}",
        f"Engagement state: {lead.get('engagement_status', 'new')}",
        "",
        "─" * 60,
    ]

    if reply_excerpt:
        parts.append("REPLY CONTENT:")
        parts.append("")
        parts.append(reply_excerpt[:2000])
        parts.append("")
        parts.append("─" * 60)

    parts.append("")
    parts.append(brief)
    parts.append("")
    parts.append("─" * 60)
    parts.append("")
    parts.append("ACTION: This lead just engaged. Reply within 4 hours for best conversion.")
    parts.append(f"Original email is in the inbox — search: from:{lead.get('email','')}")

    return "\n".join(parts)


def _send_alert_smtp(
    sender_email: str,
    sender_name: str,
    app_password: str,
    notify_email: str,
    subject: str,
    body: str,
) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{sender_name} <{sender_email}>"
    msg["To"] = notify_email
    msg.set_content(body)

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(GMAIL_SMTP_HOST, GMAIL_SMTP_PORT, context=ctx) as server:
        server.login(sender_email, app_password)
        server.send_message(msg)


async def send_reply_alert(
    lead: dict[str, Any],
    cfg: dict[str, Any],
    reply_excerpt: str = "",
) -> bool:
    """Send Ben a notification with full lead intel when a reply comes in."""
    if not cfg.get("enableReplyAlerts", True):
        return False

    sender_email = cfg.get("senderEmail")
    app_password = cfg.get("gmailAppPassword")
    notify_email = cfg.get("alertNotifyEmail") or sender_email

    if cfg.get("dryRun", True):
        Actor.log.info(
            f"[DRY RUN] Would send reply alert to {notify_email} for {lead.get('company_name')}"
        )
        return True

    if not sender_email or not app_password:
        Actor.log.warning("Reply alert skipped — missing SMTP credentials")
        return False

    company = lead.get("company_name", "Unknown")
    contact = lead.get("contact_name") or lead.get("email", "lead")
    subject = f"🔔 Reply from {contact} — {company}"

    body = _build_alert_body(lead, reply_excerpt)

    try:
        await asyncio.to_thread(
            _send_alert_smtp,
            sender_email,
            cfg.get("senderName", sender_email),
            app_password,
            notify_email,
            subject,
            body,
        )
        Actor.log.info(f"Reply alert sent to {notify_email} for {company}")
        return True
    except smtplib.SMTPException as e:
        Actor.log.error(f"Failed to send reply alert: {e}")
        return False
    except Exception as e:
        Actor.log.exception(f"Unexpected error in reply alert: {e}")
        return False


async def send_engagement_alert(
    lead: dict[str, Any],
    event_type: str,
    cfg: dict[str, Any],
) -> bool:
    """Send alert for high-value engagement events: clicked, registered.

    Skipped for opens (too noisy). Triggered by tracking endpoints.
    """
    if not cfg.get("enableEngagementAlerts", True):
        return False
    if event_type not in ("clicked", "registered"):
        return False

    sender_email = cfg.get("senderEmail")
    app_password = cfg.get("gmailAppPassword")
    notify_email = cfg.get("alertNotifyEmail") or sender_email

    if cfg.get("dryRun", True) or not sender_email or not app_password:
        return False

    company = lead.get("company_name", "Unknown")
    if event_type == "registered":
        subject = f"✅ REGISTERED: {company} — Loggers Summit"
        action_line = "They registered for the Summit. Send personal welcome + hotel info + ask about 1:1 at event."
    else:
        subject = f"🔗 Click: {company} — engaged with Summit registration link"
        action_line = "They clicked but haven't registered yet. Likely a question about logistics or fit."

    brief = build_intel_brief(lead)
    body = f"""ENGAGEMENT EVENT: {event_type.upper()}

Lead: {company}
Contact: {lead.get('contact_name') or 'unknown'}
Email: {lead.get('email')}

ACTION: {action_line}

{'─' * 60}

{brief}
"""

    try:
        await asyncio.to_thread(
            _send_alert_smtp,
            sender_email,
            cfg.get("senderName", sender_email),
            app_password,
            notify_email,
            subject,
            body,
        )
        Actor.log.info(f"Engagement alert ({event_type}) sent for {company}")
        return True
    except Exception as e:
        Actor.log.error(f"Engagement alert failed: {e}")
        return False
